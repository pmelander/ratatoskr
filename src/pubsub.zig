//! pubsub — topic-based publish/subscribe message broker.
//!
//! Ports Subtopic's bubble-up hierarchical routing to Zig.
//! Topics are `/`-delimited strings (e.g. "app/ui/button").
//! Publishing to a topic delivers to subscribers of that topic AND every
//! ancestor topic, walking up toward the root:
//!
//!   publish("app/ui/button", data)
//!     → notifies subscribers of "app/ui/button"
//!     → notifies subscribers of "app/ui"
//!     → notifies subscribers of "app"
//!
//! **Topic rules**
//!   - Segments are separated by `TOPIC_SEPARATOR` (`/`).
//!   - An empty string is a valid (root-only) topic; it has no ancestors.
//!   - Trailing separators create an empty final segment which is treated as
//!     a distinct topic level (e.g. `"app/"` ≠ `"app"`). Avoid them.
//!
//! No global state; all state lives in `Broker`.
//! Not thread-safe — callers own synchronisation.

const std = @import("std");

/// Separator character used to delimit topic hierarchy levels.
pub const TOPIC_SEPARATOR: u8 = '/';

/// Type-erased callback invoked when a matching publication arrives.
///
/// Parameters:
///   `topic` — the ancestor-or-equal topic at which this subscriber was
///             registered.  When a deep publish triggers an ancestor
///             subscription, this is the *ancestor* key, not the originally
///             published topic string.
///   `data`  — caller-supplied payload passed to `Broker.publish`; may be null.
///   `ctx`   — per-subscriber context pointer registered at subscribe time.
pub const CallbackFn = *const fn (
    topic: []const u8,
    data: ?*anyopaque,
    ctx: ?*anyopaque,
) void;

/// Opaque subscription token returned by `Broker.subscribe`.
/// Pass to `Broker.unsubscribe` to cancel the subscription.
/// The `topic` slice is the same memory passed to `subscribe`; it must remain
/// valid for the lifetime of the handle.
pub const Handle = struct {
    topic: []const u8,
    id: u64,
};

/// Internal record stored per registered subscriber.
const Subscriber = struct {
    id: u64,
    callback: CallbackFn,
    ctx: ?*anyopaque,
};

/// In Zig 0.16, std.ArrayList is the allocator-unmanaged variant:
/// initialise with `.empty`, pass the allocator to every mutating call.
const SubList = std.ArrayList(Subscriber);

/// Configuration options for `Broker`.
pub const BrokerOptions = struct {
    /// When `true`, the broker copies topic strings into its own allocation.
    /// Subscribe handles are then valid regardless of the caller's topic
    /// string lifetime.
    ///
    /// When `false` (default), topic slices are borrowed — the caller must
    /// keep them alive for the lifetime of every Handle returned by `subscribe`.
    copy_topics: bool = false,
};

/// Returns a Broker type configured by `options`.
///
/// The default (non-copying) type is pre-configured as `ratatoskr.Broker` in
/// the package root.  For a copying variant:
///
///   const ratatoskr = @import("ratatoskr");
///   var broker = ratatoskr.BrokerWith(.{ .copy_topics = true }).init(alloc);
///
/// **Thread safety**: not thread-safe; callers own synchronisation.
///
/// **Re-entrancy**: calling `publish` from within a callback dispatched by the
/// same broker is not supported and triggers a `std.debug.assert` failure in
/// debug and ReleaseSafe builds.  Design callbacks to defer follow-up publishes
/// (e.g. via a queue) rather than calling back into the broker directly.
pub fn Broker(comptime options: BrokerOptions) type {
    return struct {
        const Self = @This();

        allocator: std.mem.Allocator,
        /// Map from topic string → ordered list of active subscribers.
        /// Insertion order within each list is preserved; `publish` iterates in
        /// reverse to achieve LIFO (most-recently-subscribed fires first).
        ///
        /// When `options.copy_topics` is true, the map owns its key strings.
        cache: std.StringHashMap(SubList),
        /// Monotonically increasing counter used to generate unique Handle ids.
        next_id: u64,
        /// Set to `true` for the duration of a `publish` call.
        /// Used to detect and reject re-entrant publishes in debug builds.
        publishing: bool,

        /// Initialise a Broker backed by `allocator`.
        /// Call `deinit` to release all resources.
        pub fn init(allocator: std.mem.Allocator) Self {
            return .{
                .allocator = allocator,
                .cache = std.StringHashMap(SubList).init(allocator),
                .next_id = 0,
                .publishing = false,
            };
        }

        /// Release all resources owned by this Broker.
        /// Any outstanding Handles are invalidated.
        pub fn deinit(self: *Self) void {
            var it = self.cache.iterator();
            while (it.next()) |entry| {
                entry.value_ptr.deinit(self.allocator);
                if (comptime options.copy_topics) {
                    self.allocator.free(entry.key_ptr.*);
                }
            }
            self.cache.deinit();
        }

        /// Register `callback`+`ctx` as a subscriber for `topic`.
        ///
        /// Returns a `Handle` that uniquely identifies this subscription.
        ///
        /// When `options.copy_topics` is false (default), `topic` memory must
        /// outlive the Handle.  When `options.copy_topics` is true, the broker
        /// copies the topic string and manages its lifetime independently.
        ///
        /// Errors:
        ///   `error.OutOfMemory` — allocation failure; broker state is unchanged.
        pub fn subscribe(
            self: *Self,
            topic: []const u8,
            callback: CallbackFn,
            ctx: ?*anyopaque,
        ) error{OutOfMemory}!Handle {
            // Acquire the map entry, duplicating the key when copy_topics is set.
            const gop = blk: {
                if (comptime options.copy_topics) {
                    const owned = try self.allocator.dupe(u8, topic);
                    const result = self.cache.getOrPut(owned) catch |err| {
                        self.allocator.free(owned);
                        return err;
                    };
                    // If the topic already existed, free the duplicate we just
                    // created — the map retains its pre-existing owned key.
                    if (result.found_existing) self.allocator.free(owned);
                    break :blk result;
                } else {
                    break :blk try self.cache.getOrPut(topic);
                }
            };
            if (!gop.found_existing) {
                gop.value_ptr.* = .empty;
            }
            const id = self.next_id;
            gop.value_ptr.append(self.allocator, .{
                .id = id,
                .callback = callback,
                .ctx = ctx,
            }) catch |err| {
                // Roll back the freshly-created map entry to keep state consistent.
                if (!gop.found_existing) {
                    gop.value_ptr.deinit(self.allocator);
                    if (self.cache.fetchRemove(gop.key_ptr.*)) |kv| {
                        if (comptime options.copy_topics) self.allocator.free(kv.key);
                    }
                }
                return err;
            };
            self.next_id += 1;
            return .{ .topic = gop.key_ptr.*, .id = id };
        }

        /// Cancel the subscription identified by `handle`.
        /// Safe to call with a stale or already-unsubscribed handle (no-op).
        pub fn unsubscribe(self: *Self, handle: Handle) void {
            const list = self.cache.getPtr(handle.topic) orelse return;
            for (list.items, 0..) |sub, i| {
                if (sub.id == handle.id) {
                    // orderedRemove preserves insertion order so that the reverse-
                    // iteration LIFO guarantee holds after any removal.
                    _ = list.orderedRemove(i);
                    // When the last subscriber leaves, free the backing array and
                    // remove the map entry to keep the cache compact.
                    if (list.items.len == 0) {
                        if (self.cache.fetchRemove(handle.topic)) |kv| {
                            var empty = kv.value;
                            empty.deinit(self.allocator);
                            if (comptime options.copy_topics) self.allocator.free(kv.key);
                        }
                    }
                    return;
                }
            }
        }

        /// Remove ALL subscriptions for `topic`, freeing the associated list.
        pub fn clearTopic(self: *Self, topic: []const u8) void {
            if (self.cache.fetchRemove(topic)) |kv| {
                var list = kv.value;
                list.deinit(self.allocator);
                if (comptime options.copy_topics) self.allocator.free(kv.key);
            }
        }

        /// Deliver `data` to all subscribers of `topic` and every ancestor topic.
        ///
        /// Delivery order within a single topic level: most-recently-subscribed
        /// first (matching Subtopic's reverse-iteration behaviour).
        /// Ancestor topics are visited from the published topic toward the root.
        ///
        /// Calling `publish` re-entrantly from within a callback is not supported
        /// and panics in debug/ReleaseSafe builds.
        pub fn publish(
            self: *Self,
            topic: []const u8,
            data: ?*anyopaque,
        ) void {
            std.debug.assert(!self.publishing); // re-entrant publish is not allowed
            self.publishing = true;
            defer self.publishing = false;
            var current: []const u8 = topic;
            while (true) {
                if (self.cache.getPtr(current)) |list| {
                    // Iterate in reverse so the most-recently-subscribed fires first.
                    var i: usize = list.items.len;
                    while (i > 0) {
                        i -= 1;
                        list.items[i].callback(current, data, list.items[i].ctx);
                    }
                }
                // Strip the rightmost segment to walk up to the parent topic.
                const sep = std.mem.lastIndexOfScalar(u8, current, TOPIC_SEPARATOR) orelse break;
                current = current[0..sep];
            }
        }

        /// Deliver `data` to subscribers of `topic` only — no ancestor walk.
        ///
        /// Use for point-to-point delivery where ancestor topics must not receive
        /// messages intended for a specific sub-topic.
        /// Delivery order: most-recently-subscribed first (LIFO), same as `publish`.
        ///
        /// Panics on re-entrant calls in debug/ReleaseSafe builds.
        pub fn publishExact(
            self: *Self,
            topic: []const u8,
            data: ?*anyopaque,
        ) void {
            std.debug.assert(!self.publishing); // re-entrant publish is not allowed
            self.publishing = true;
            defer self.publishing = false;
            if (self.cache.getPtr(topic)) |list| {
                var i: usize = list.items.len;
                while (i > 0) {
                    i -= 1;
                    list.items[i].callback(topic, data, list.items[i].ctx);
                }
            }
        }
    };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// Convenience alias — all tests that exercise the default (non-copying)
// configuration use `B` so the call sites stay concise.
const B = Broker(.{});

// --- shared test callbacks ---

fn counterCb(topic: []const u8, data: ?*anyopaque, ctx: ?*anyopaque) void {
    _ = topic;
    _ = data;
    const n: *u32 = @ptrCast(@alignCast(ctx.?));
    n.* += 1;
}

/// Records the sequence number at which this subscriber fired, then bumps it.
const SeqCtx = struct { seq: *u32, fired_at: *u32 };
fn seqCb(topic: []const u8, data: ?*anyopaque, ctx: ?*anyopaque) void {
    _ = topic;
    _ = data;
    const sc: *SeqCtx = @ptrCast(@alignCast(ctx.?));
    sc.fired_at.* = sc.seq.*;
    sc.seq.* += 1;
}

/// Captures the topic string the subscriber was called with.
const TopicCapture = struct {
    value: []const u8 = "",
    fn cb(topic: []const u8, data: ?*anyopaque, ctx: ?*anyopaque) void {
        _ = data;
        const self: *TopicCapture = @ptrCast(@alignCast(ctx.?));
        self.value = topic;
    }
};

/// Captures the data pointer that was published.
const DataCapture = struct {
    ptr: ?*anyopaque = null,
    fn cb(topic: []const u8, data: ?*anyopaque, ctx: ?*anyopaque) void {
        _ = topic;
        const self: *DataCapture = @ptrCast(@alignCast(ctx.?));
        self.ptr = data;
    }
};

// --- Handle ---

test "Handle has topic and id fields" {
    const h: Handle = .{ .topic = "a/b", .id = 0 };
    try std.testing.expectEqualStrings("a/b", h.topic);
    try std.testing.expectEqual(@as(u64, 0), h.id);
}

// --- init / deinit ---

test "Broker.init and deinit do not leak" {
    var broker = B.init(std.testing.allocator);
    defer broker.deinit();
}

test "deinit with active subscriptions does not leak" {
    var broker = B.init(std.testing.allocator);
    var count: u32 = 0;
    _ = try broker.subscribe("a", counterCb, &count);
    _ = try broker.subscribe("a", counterCb, &count);
    _ = try broker.subscribe("b/c", counterCb, &count);
    broker.deinit(); // testing.allocator will catch any leaks
}

// --- subscribe / publish basics ---

test "subscribe and publish — exact match" {
    var broker = B.init(std.testing.allocator);
    defer broker.deinit();

    var count: u32 = 0;
    _ = try broker.subscribe("events", counterCb, &count);
    broker.publish("events", null);
    try std.testing.expectEqual(@as(u32, 1), count);
}

test "publish is a no-op when no subscribers exist" {
    var broker = B.init(std.testing.allocator);
    defer broker.deinit();
    broker.publish("nothing", null);
}

test "publish does not fire for an unrelated topic" {
    var broker = B.init(std.testing.allocator);
    defer broker.deinit();

    var count: u32 = 0;
    _ = try broker.subscribe("a", counterCb, &count);
    broker.publish("b", null);
    try std.testing.expectEqual(@as(u32, 0), count);
}

test "publish passes the data pointer through to the callback" {
    var broker = B.init(std.testing.allocator);
    defer broker.deinit();

    var cap = DataCapture{};
    _ = try broker.subscribe("d", DataCapture.cb, &cap);

    var payload: u32 = 42;
    broker.publish("d", &payload);
    const got: *u32 = @ptrCast(@alignCast(cap.ptr.?));
    try std.testing.expectEqual(@as(u32, 42), got.*);
}

test "multiple subscribers on the same topic all fire" {
    var broker = B.init(std.testing.allocator);
    defer broker.deinit();

    var a: u32 = 0;
    var b: u32 = 0;
    var c: u32 = 0;
    _ = try broker.subscribe("t", counterCb, &a);
    _ = try broker.subscribe("t", counterCb, &b);
    _ = try broker.subscribe("t", counterCb, &c);

    broker.publish("t", null);
    try std.testing.expectEqual(@as(u32, 1), a);
    try std.testing.expectEqual(@as(u32, 1), b);
    try std.testing.expectEqual(@as(u32, 1), c);
}

test "delivery order within a topic is LIFO" {
    var broker = B.init(std.testing.allocator);
    defer broker.deinit();

    var seq: u32 = 0;
    var fired_at_1: u32 = 999;
    var fired_at_2: u32 = 999;
    var ctx1 = SeqCtx{ .seq = &seq, .fired_at = &fired_at_1 };
    var ctx2 = SeqCtx{ .seq = &seq, .fired_at = &fired_at_2 };

    _ = try broker.subscribe("ord", seqCb, &ctx1); // subscribed first  → fires second
    _ = try broker.subscribe("ord", seqCb, &ctx2); // subscribed second → fires first

    broker.publish("ord", null);
    try std.testing.expectEqual(@as(u32, 0), fired_at_2); // fired at seq=0
    try std.testing.expectEqual(@as(u32, 1), fired_at_1); // fired at seq=1
}

test "LIFO order is preserved after an intermediate unsubscribe" {
    // Regression: swapRemove moved the last element into the removed slot,
    // silently permuting the order seen by reverse iteration.
    // orderedRemove must be used so the relative registration order is stable.
    var broker = B.init(std.testing.allocator);
    defer broker.deinit();

    var seq: u32 = 0;
    var fired_at_1: u32 = 999;
    var fired_at_2: u32 = 999;
    var fired_at_3: u32 = 999;
    var ctx1 = SeqCtx{ .seq = &seq, .fired_at = &fired_at_1 };
    var ctx2 = SeqCtx{ .seq = &seq, .fired_at = &fired_at_2 };
    var ctx3 = SeqCtx{ .seq = &seq, .fired_at = &fired_at_3 };

    const h1 = try broker.subscribe("t", seqCb, &ctx1); // index 0 — registered 1st
    _ = try broker.subscribe("t", seqCb, &ctx2); // index 1 — registered 2nd
    _ = try broker.subscribe("t", seqCb, &ctx3); // index 2 — registered 3rd

    // Remove the first-registered subscriber.  With swapRemove this would
    // move ctx3 to index 0, breaking LIFO for the remaining two.
    broker.unsubscribe(h1);

    seq = 0;
    broker.publish("t", null);

    // Expected LIFO: ctx3 (registered 3rd) fires at seq=0, ctx2 at seq=1.
    try std.testing.expectEqual(@as(u32, 0), fired_at_3);
    try std.testing.expectEqual(@as(u32, 1), fired_at_2);
    try std.testing.expectEqual(@as(u32, 999), fired_at_1); // never fired
}

// --- hierarchical bubble-up ---

test "hierarchical: full ancestor chain fires on deep publish" {
    var broker = B.init(std.testing.allocator);
    defer broker.deinit();

    var app: u32 = 0;
    var ui: u32 = 0;
    var btn: u32 = 0;
    _ = try broker.subscribe("app", counterCb, &app);
    _ = try broker.subscribe("app/ui", counterCb, &ui);
    _ = try broker.subscribe("app/ui/button", counterCb, &btn);

    broker.publish("app/ui/button", null);
    try std.testing.expectEqual(@as(u32, 1), app);
    try std.testing.expectEqual(@as(u32, 1), ui);
    try std.testing.expectEqual(@as(u32, 1), btn);
}

test "hierarchical: sibling topic does not fire" {
    var broker = B.init(std.testing.allocator);
    defer broker.deinit();

    var count: u32 = 0;
    _ = try broker.subscribe("app/ui", counterCb, &count);
    broker.publish("app/other", null);
    try std.testing.expectEqual(@as(u32, 0), count);
}

test "hierarchical: parent publish does not trickle down to child subscriber" {
    var broker = B.init(std.testing.allocator);
    defer broker.deinit();

    var count: u32 = 0;
    _ = try broker.subscribe("app/ui/button", counterCb, &count);
    broker.publish("app/ui", null);
    try std.testing.expectEqual(@as(u32, 0), count);
}

test "hierarchical: single-segment publish fires only that topic" {
    var broker = B.init(std.testing.allocator);
    defer broker.deinit();

    var root: u32 = 0;
    var child: u32 = 0;
    _ = try broker.subscribe("app", counterCb, &root);
    _ = try broker.subscribe("app/ui", counterCb, &child);

    broker.publish("app", null);
    try std.testing.expectEqual(@as(u32, 1), root);
    try std.testing.expectEqual(@as(u32, 0), child);
}

test "hierarchical: callback receives its subscribed topic, not the published topic" {
    var broker = B.init(std.testing.allocator);
    defer broker.deinit();

    var cap = TopicCapture{};
    _ = try broker.subscribe("app", TopicCapture.cb, &cap);
    broker.publish("app/ui/button", null);
    try std.testing.expectEqualStrings("app", cap.value);
}

// --- unsubscribe ---

test "unsubscribe stops future delivery" {
    var broker = B.init(std.testing.allocator);
    defer broker.deinit();

    var count: u32 = 0;
    const h = try broker.subscribe("x", counterCb, &count);

    broker.publish("x", null);
    try std.testing.expectEqual(@as(u32, 1), count);

    broker.unsubscribe(h);
    broker.publish("x", null);
    try std.testing.expectEqual(@as(u32, 1), count); // unchanged
}

test "unsubscribe with a stale handle is a no-op" {
    var broker = B.init(std.testing.allocator);
    defer broker.deinit();

    var count: u32 = 0;
    const h = try broker.subscribe("x", counterCb, &count);
    broker.unsubscribe(h);
    broker.unsubscribe(h); // second call must not crash
}

test "unsubscribe removes only the targeted subscriber, leaving others intact" {
    var broker = B.init(std.testing.allocator);
    defer broker.deinit();

    var a: u32 = 0;
    var b: u32 = 0;
    const ha = try broker.subscribe("t", counterCb, &a);
    _ = try broker.subscribe("t", counterCb, &b);

    broker.unsubscribe(ha);
    broker.publish("t", null);
    try std.testing.expectEqual(@as(u32, 0), a); // removed
    try std.testing.expectEqual(@as(u32, 1), b); // untouched
}

test "unsubscribe last subscriber frees map entry — no leak and publish is safe" {
    var broker = B.init(std.testing.allocator);
    defer broker.deinit();

    var count: u32 = 0;
    const h = try broker.subscribe("x", counterCb, &count);
    broker.unsubscribe(h);

    // Map entry must be gone; publish must not crash.
    broker.publish("x", null);
    try std.testing.expectEqual(@as(u32, 0), count);
    // deinit (defer) confirms the backing array was freed and not leaked.
}

// --- clearTopic ---

test "clearTopic removes all subscribers for that topic" {
    var broker = B.init(std.testing.allocator);
    defer broker.deinit();

    var a: u32 = 0;
    var b: u32 = 0;
    _ = try broker.subscribe("ch", counterCb, &a);
    _ = try broker.subscribe("ch", counterCb, &b);

    broker.clearTopic("ch");
    broker.publish("ch", null);
    try std.testing.expectEqual(@as(u32, 0), a);
    try std.testing.expectEqual(@as(u32, 0), b);
}

test "clearTopic does not affect subscribers on other topics" {
    var broker = B.init(std.testing.allocator);
    defer broker.deinit();

    var a: u32 = 0;
    var b: u32 = 0;
    _ = try broker.subscribe("alpha", counterCb, &a);
    _ = try broker.subscribe("beta", counterCb, &b);

    broker.clearTopic("alpha");
    broker.publish("alpha", null);
    broker.publish("beta", null);
    try std.testing.expectEqual(@as(u32, 0), a);
    try std.testing.expectEqual(@as(u32, 1), b);
}

test "clearTopic on an unknown topic is a no-op" {
    var broker = B.init(std.testing.allocator);
    defer broker.deinit();
    broker.clearTopic("never/subscribed");
}

// --- OOM safety ---

test "subscribe is OOM-safe at every allocation point" {
    // Iterate through every possible allocation failure index until subscribe
    // finally succeeds.  After each failure, the broker must be cleanly
    // deinitable with no leaks (verified by the testing allocator).
    var fail_index: usize = 0;
    while (true) : (fail_index += 1) {
        var fa = std.testing.FailingAllocator.init(
            std.testing.allocator,
            .{ .fail_index = fail_index },
        );
        var broker = B.init(fa.allocator());
        var count: u32 = 0;
        if (broker.subscribe("x", counterCb, &count)) |_| {
            // subscribe succeeded — all failure points have been covered.
            broker.deinit();
            break;
        } else |_| {
            broker.deinit(); // must not crash or leak on any OOM path
        }
    }
}

// --- re-entrancy guard ---

test "re-entrancy guard: publishing flag clears after publish returns" {
    // Verifies that sequential publishes succeed — the guard flag is correctly
    // reset via defer even when the subscriber list is non-empty.
    var broker = B.init(std.testing.allocator);
    defer broker.deinit();

    var count: u32 = 0;
    _ = try broker.subscribe("t", counterCb, &count);
    broker.publish("t", null);
    broker.publish("t", null); // must not trigger the re-entrancy assert
    try std.testing.expectEqual(@as(u32, 2), count);
}

test "re-entrancy guard: flag is false on a broker that has never published" {
    var broker = B.init(std.testing.allocator);
    defer broker.deinit();
    try std.testing.expect(!broker.publishing);
}

test "re-entrancy guard: flag is false after publish with no subscribers" {
    var broker = B.init(std.testing.allocator);
    defer broker.deinit();
    broker.publish("empty", null);
    try std.testing.expect(!broker.publishing);
}

// --- publishExact ---

test "publishExact fires exact topic only — not ancestors" {
    var broker = B.init(std.testing.allocator);
    defer broker.deinit();

    var root: u32 = 0;
    var child: u32 = 0;
    _ = try broker.subscribe("a", counterCb, &root);
    _ = try broker.subscribe("a/b", counterCb, &child);

    broker.publishExact("a/b", null);
    try std.testing.expectEqual(@as(u32, 0), root); // ancestor must NOT fire
    try std.testing.expectEqual(@as(u32, 1), child); // exact match fires
}

test "publishExact does not trickle down to children" {
    var broker = B.init(std.testing.allocator);
    defer broker.deinit();

    var child: u32 = 0;
    _ = try broker.subscribe("a/b/c", counterCb, &child);

    broker.publishExact("a/b", null);
    try std.testing.expectEqual(@as(u32, 0), child);
}

test "publishExact on a topic with no subscribers is a no-op" {
    var broker = B.init(std.testing.allocator);
    defer broker.deinit();
    broker.publishExact("never/subscribed", null);
}

test "publishExact LIFO order matches publish" {
    var broker = B.init(std.testing.allocator);
    defer broker.deinit();

    var seq: u32 = 0;
    var fired_at_1: u32 = 999;
    var fired_at_2: u32 = 999;
    var ctx1 = SeqCtx{ .seq = &seq, .fired_at = &fired_at_1 };
    var ctx2 = SeqCtx{ .seq = &seq, .fired_at = &fired_at_2 };

    _ = try broker.subscribe("t", seqCb, &ctx1); // subscribed first  → fires second
    _ = try broker.subscribe("t", seqCb, &ctx2); // subscribed second → fires first

    broker.publishExact("t", null);
    try std.testing.expectEqual(@as(u32, 0), fired_at_2);
    try std.testing.expectEqual(@as(u32, 1), fired_at_1);
}

test "publishExact re-entrancy guard: flag clears after return" {
    var broker = B.init(std.testing.allocator);
    defer broker.deinit();

    var count: u32 = 0;
    _ = try broker.subscribe("t", counterCb, &count);
    broker.publishExact("t", null);
    broker.publishExact("t", null); // second call must not hit the assert
    try std.testing.expectEqual(@as(u32, 2), count);
}

// --- copy_topics option ---

test "copy_topics=true: topic lifetime is independent of the caller's string" {
    // Allocate a heap string, subscribe, free the string, then publish —
    // the broker must have made its own copy so there is no use-after-free.
    const C = Broker(.{ .copy_topics = true });
    var broker = C.init(std.testing.allocator);
    defer broker.deinit();

    const heap_topic = try std.testing.allocator.dupe(u8, "sensor/temp");
    var count: u32 = 0;
    const h = try broker.subscribe(heap_topic, counterCb, &count);
    std.testing.allocator.free(heap_topic); // free the original — broker owns its copy

    // Publish using a string literal with the same content; broker matches by value.
    broker.publish("sensor/temp", null);
    try std.testing.expectEqual(@as(u32, 1), count);

    broker.unsubscribe(h);
    broker.publish("sensor/temp", null);
    try std.testing.expectEqual(@as(u32, 1), count); // no longer subscribed
}

test "copy_topics=true: subscribe same topic twice reuses the stored key" {
    const C = Broker(.{ .copy_topics = true });
    var broker = C.init(std.testing.allocator);
    defer broker.deinit();

    var a: u32 = 0;
    var b: u32 = 0;
    _ = try broker.subscribe("t", counterCb, &a);
    _ = try broker.subscribe("t", counterCb, &b); // second sub reuses existing key

    broker.publish("t", null);
    try std.testing.expectEqual(@as(u32, 1), a);
    try std.testing.expectEqual(@as(u32, 1), b);
}

test "copy_topics=true: unsubscribe last subscriber frees owned key — no leak" {
    const C = Broker(.{ .copy_topics = true });
    var broker = C.init(std.testing.allocator);
    defer broker.deinit();

    var count: u32 = 0;
    const h = try broker.subscribe("x/y", counterCb, &count);
    broker.unsubscribe(h);
    // testing.allocator verifies the key was freed (no leak on deinit).
    broker.publish("x/y", null);
    try std.testing.expectEqual(@as(u32, 0), count);
}

test "copy_topics=true: clearTopic frees owned key — no leak" {
    const C = Broker(.{ .copy_topics = true });
    var broker = C.init(std.testing.allocator);
    defer broker.deinit();

    var count: u32 = 0;
    _ = try broker.subscribe("a/b", counterCb, &count);
    _ = try broker.subscribe("a/b", counterCb, &count);
    broker.clearTopic("a/b");
    // testing.allocator confirms the key allocation was freed.
}

test "copy_topics=true: deinit with active subscriptions does not leak" {
    const C = Broker(.{ .copy_topics = true });
    var broker = C.init(std.testing.allocator);
    var count: u32 = 0;
    _ = try broker.subscribe("p/q", counterCb, &count);
    _ = try broker.subscribe("r", counterCb, &count);
    broker.deinit(); // testing.allocator verifies all keys and lists are freed
}

test "copy_topics=true: subscribe is OOM-safe at every allocation point" {
    const C = Broker(.{ .copy_topics = true });
    var fail_index: usize = 0;
    while (true) : (fail_index += 1) {
        var fa = std.testing.FailingAllocator.init(
            std.testing.allocator,
            .{ .fail_index = fail_index },
        );
        var broker = C.init(fa.allocator());
        var count: u32 = 0;
        if (broker.subscribe("t", counterCb, &count)) |_| {
            broker.deinit();
            break;
        } else |_| {
            broker.deinit();
        }
    }
}
