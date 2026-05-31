//! static_broker — zero-allocation, freestanding-safe publish/subscribe broker.
//!
//! ## Design note
//!
//! ### What it does and why it belongs here
//!
//! `StaticBroker(topics, max_subs)` is a comptime-parametrised broker whose
//! topic set and per-topic subscriber capacity are fixed at compile time.
//! It never calls an allocator, uses only stack and `.bss` / global storage,
//! and is safe for freestanding/bare-metal targets where no heap exists.
//!
//! It complements `Broker` (heap-backed, dynamic topics) for environments
//! such as RTOS tasks, microcontrollers, and safety-critical systems where
//! worst-case memory usage must be statically bounded and verifiable without
//! running the program.
//!
//! ### Public types and function signatures
//!
//! ```
//! pub fn StaticBroker(
//!     comptime topics: []const []const u8,
//!     comptime max_subscribers_per_topic: usize,
//! ) type
//! ```
//!
//! The returned type exposes:
//!
//! ```
//! pub fn init() Self
//! pub fn subscribe(topic, callback, ctx) error{TopicNotFound,TooManySubscribers}!Handle
//! pub fn unsubscribe(handle) void
//! pub fn subscribeOnce(topic, callback, ctx, *OnceCtx) error{TopicNotFound,TooManySubscribers}!Handle
//! pub fn clearTopic(topic) void
//! pub fn clearAll() void
//! pub fn subscriberCount(topic) usize
//! pub fn publish(topic, data) void          // hierarchical bubble-up
//! pub fn publishExact(topic, data) void     // exact match only
//! pub const OnceCtx: type                   // caller-provided once storage
//! ```
//!
//! `Handle` and `CallbackFn` are the same types as in `Broker`.
//!
//! ### Allocator strategy
//!
//! None. All storage lives in the struct itself:
//!
//!   `[topics.len][max_subscribers_per_topic]Subscriber` (inline array of arrays)
//!
//! Topic strings are compile-time constants (pointer-stable for the program
//! lifetime). `Handle.topic` always points into the `topics` slice, never
//! into caller-owned memory.
//!
//! ### Comptime constraints and platform gates
//!
//! - `topics` must be a comptime-known slice of string literals.
//!   Duplicate entries compile fine but waste slots; deduplication is the
//!   caller's responsibility.
//! - `max_subscribers_per_topic = 0` is legal (every `subscribe` returns
//!   `TooManySubscribers`).
//! - `topics.len = 0` is legal (every `subscribe` returns `TopicNotFound`).
//! - No `std.os` calls; no allocator; compiles for `freestanding` targets.
//! - Re-entrancy guard (`publishing: bool`) matches `Broker` semantics:
//!   nested `publish`/`publishExact` panics in debug and ReleaseSafe.
//!   Calling `unsubscribe` from within a callback is safe (used by
//!   `subscribeOnce`).
//!
//! ### Limitations vs `Broker`
//!
//! - Topic set is immutable after comptime instantiation.
//! - Publishing to a topic not in `topics` is a no-op (no panic); publishing
//!   to an ancestor that IS in `topics` fires that ancestor normally.
//! - No `topicIterator` — callers already know the static topic set.
//! - No `copy_topics` option — all topics are compile-time constants.
//!
//! ### Example
//!
//! ```zig
//! const Events = StaticBroker(
//!     &.{ "sensors/temp", "sensors/pressure", "ui/button" },
//!     8,
//! );
//! var broker = Events.init();
//!
//! const h = try broker.subscribe("sensors/temp", onTemp, null);
//! broker.publish("sensors/temp", &reading);
//! broker.unsubscribe(h);
//! ```

const std = @import("std");
const pubsub = @import("pubsub.zig");

pub const Handle = pubsub.Handle;
pub const CallbackFn = pubsub.CallbackFn;
pub const TOPIC_SEPARATOR = pubsub.TOPIC_SEPARATOR;

/// Internal subscriber record — identical layout to the one in pubsub.zig but
/// kept private so each module owns its own type.
const Subscriber = struct {
    id: u64,
    callback: CallbackFn,
    ctx: ?*anyopaque,
};

/// Returns a zero-allocation, freestanding-safe broker type.
///
/// Parameters:
///   `topics`                    — comptime slice of topic strings that the
///                                 broker recognises.  Publishing to a topic
///                                 not in this set is silently ignored for
///                                 that level, but ancestor topics that ARE
///                                 in the set still fire normally.
///   `max_subscribers_per_topic` — maximum number of concurrent subscribers
///                                 per topic.  Attempting to exceed this
///                                 returns `error.TooManySubscribers`.
///
/// Thread safety and re-entrancy: same rules as `Broker` — not thread-safe;
/// nested `publish`/`publishExact` panics in debug/ReleaseSafe builds.
pub fn StaticBroker(
    comptime topics: []const []const u8,
    comptime max_subscribers_per_topic: usize,
) type {
    return struct {
        const Self = @This();

        /// Minimal fixed-capacity subscriber list — no allocator needed.
        const SubList = struct {
            buffer: [max_subscribers_per_topic]Subscriber = undefined,
            len: usize = 0,

            fn slice(self: *SubList) []Subscriber {
                return self.buffer[0..self.len];
            }

            fn append(self: *SubList, sub: Subscriber) error{TooManySubscribers}!void {
                if (self.len >= max_subscribers_per_topic) return error.TooManySubscribers;
                self.buffer[self.len] = sub;
                self.len += 1;
            }

            /// Remove element at `i`, shifting later elements left.
            /// Preserves insertion order so reverse-iteration LIFO is stable.
            fn orderedRemove(self: *SubList, i: usize) void {
                std.debug.assert(i < self.len);
                std.mem.copyForwards(
                    Subscriber,
                    self.buffer[i .. self.len - 1],
                    self.buffer[i + 1 .. self.len],
                );
                self.len -= 1;
            }
        };

        /// One subscriber list per declared topic, laid out inline.
        lists: [topics.len]SubList,
        /// Monotonically increasing counter for unique Handle ids.
        next_id: u64,
        /// Re-entrancy guard: true while `publish` or `publishExact` is executing.
        publishing: bool,

        /// Initialise a StaticBroker.  No allocator required.
        pub fn init() Self {
            return .{
                .lists = [1]SubList{.{}} ** topics.len,
                .next_id = 0,
                .publishing = false,
            };
        }

        /// Register `callback`+`ctx` as a subscriber for `topic`.
        ///
        /// Returns a `Handle` that uniquely identifies this subscription.
        /// `Handle.topic` always points to the compile-time constant in
        /// `topics`; it is valid for the program lifetime.
        ///
        /// Errors:
        ///   `error.TopicNotFound`      — `topic` is not in the comptime topic list.
        ///   `error.TooManySubscribers` — the per-topic slot array is full.
        pub fn subscribe(
            self: *Self,
            topic: []const u8,
            callback: CallbackFn,
            ctx: ?*anyopaque,
        ) error{ TopicNotFound, TooManySubscribers }!Handle {
            // Comptime guard: when topics is empty the list array has zero length;
            // prevent the compiler from type-checking the indexing code below.
            if (comptime topics.len == 0) return error.TopicNotFound;
            const idx = topicIndex(topic) orelse return error.TopicNotFound;
            const id = self.next_id;
            std.debug.assert(id != std.math.maxInt(u64)); // id exhaustion guard
            try self.lists[idx].append(.{ .id = id, .callback = callback, .ctx = ctx });
            self.next_id += 1;
            return .{ .topic = topics[idx], .id = id };
        }

        /// Cancel the subscription identified by `handle`.
        /// Safe to call with a stale or already-unsubscribed handle (no-op).
        pub fn unsubscribe(self: *Self, handle: Handle) void {
            if (comptime topics.len == 0) return;
            const idx = topicIndex(handle.topic) orelse return;
            const list = &self.lists[idx];
            for (list.slice(), 0..) |sub, i| {
                if (sub.id == handle.id) {
                    list.orderedRemove(i);
                    return;
                }
            }
        }

        /// Storage for a `subscribeOnce` registration.
        ///
        /// Declare one of these (typically stack-allocated) and keep it alive
        /// until the callback fires or you manually unsubscribe.
        pub const OnceCtx = struct {
            broker: *Self,
            handle: Handle,
            callback: CallbackFn,
            ctx: ?*anyopaque,
        };

        fn onceFn(topic: []const u8, data: ?*anyopaque, ctx: ?*anyopaque) void {
            const oc: *OnceCtx = @ptrCast(@alignCast(ctx.?));
            // Unsubscribe before invoking user callback so re-subscribing
            // from within the callback works without interference.
            oc.broker.unsubscribe(oc.handle);
            oc.callback(topic, data, oc.ctx);
        }

        /// Subscribe `callback`+`ctx` for a single delivery on `topic`.
        /// After the first publish that matches, the subscription is automatically
        /// cancelled before `callback` is invoked.
        ///
        /// `once_buf` must remain valid until the callback fires (or until you
        /// manually call `unsubscribe(returned_handle)` to cancel early).
        ///
        /// Errors: same as `subscribe`.
        pub fn subscribeOnce(
            self: *Self,
            topic: []const u8,
            callback: CallbackFn,
            ctx: ?*anyopaque,
            once_buf: *OnceCtx,
        ) error{ TopicNotFound, TooManySubscribers }!Handle {
            once_buf.* = .{
                .broker = self,
                .handle = undefined,
                .callback = callback,
                .ctx = ctx,
            };
            const handle = try self.subscribe(topic, onceFn, @ptrCast(once_buf));
            once_buf.handle = handle;
            return handle;
        }

        /// Remove ALL subscriptions for `topic`.
        /// No-op if `topic` is not in the comptime topic list.
        pub fn clearTopic(self: *Self, topic: []const u8) void {
            if (comptime topics.len == 0) return;
            const idx = topicIndex(topic) orelse return;
            self.lists[idx].len = 0;
        }

        /// Remove ALL subscriptions across every topic.
        pub fn clearAll(self: *Self) void {
            for (&self.lists) |*list| {
                list.len = 0;
            }
        }

        /// Returns the number of active subscribers on exactly `topic`.
        /// Returns 0 if `topic` is not in the comptime topic list.
        pub fn subscriberCount(self: *const Self, topic: []const u8) usize {
            if (comptime topics.len == 0) return 0;
            const idx = topicIndex(topic) orelse return 0;
            return self.lists[idx].len;
        }

        /// Deliver `data` to all subscribers of `topic` and every ancestor topic
        /// that appears in the comptime topic list.
        ///
        /// Delivery order within a topic level: most-recently-subscribed first (LIFO).
        /// Ancestor topics are visited from the published topic toward the root.
        ///
        /// Panics on re-entrant calls in debug/ReleaseSafe builds.
        pub fn publish(self: *Self, topic: []const u8, data: ?*anyopaque) void {
            std.debug.assert(!self.publishing);
            self.publishing = true;
            defer self.publishing = false;
            var current: []const u8 = topic;
            while (true) {
                if (comptime topics.len > 0) {
                    if (topicIndex(current)) |idx| {
                        var i: usize = self.lists[idx].len;
                        while (i > 0) {
                            i -= 1;
                            // Snapshot before the callback so that a self-unsubscribe
                            // inside the callback does not invalidate the captured data.
                            const sub = self.lists[idx].buffer[i];
                            sub.callback(current, data, sub.ctx);
                        }
                    }
                }
                const sep = std.mem.lastIndexOfScalar(u8, current, TOPIC_SEPARATOR) orelse break;
                current = current[0..sep];
            }
        }

        /// Deliver `data` to subscribers of `topic` only — no ancestor walk.
        ///
        /// Panics on re-entrant calls in debug/ReleaseSafe builds.
        pub fn publishExact(self: *Self, topic: []const u8, data: ?*anyopaque) void {
            std.debug.assert(!self.publishing);
            self.publishing = true;
            defer self.publishing = false;
            if (comptime topics.len > 0) {
                if (topicIndex(topic)) |idx| {
                    var i: usize = self.lists[idx].len;
                    while (i > 0) {
                        i -= 1;
                        const sub = self.lists[idx].buffer[i];
                        sub.callback(topic, data, sub.ctx);
                    }
                }
            }
        }

        /// O(n) linear scan over the comptime topic array.
        /// For typical embedded topic counts (< 64) this is faster than a hash
        /// lookup due to cache locality and zero heap traffic.
        fn topicIndex(topic: []const u8) ?usize {
            for (topics, 0..) |t, i| {
                if (std.mem.eql(u8, t, topic)) return i;
            }
            return null;
        }
    };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// Convenience alias — three topics, up to 4 subscribers each.
const T3 = StaticBroker(&.{ "a", "a/b", "a/b/c" }, 4);

fn counterCb(topic: []const u8, data: ?*anyopaque, ctx: ?*anyopaque) void {
    _ = topic;
    _ = data;
    const n: *u32 = @ptrCast(@alignCast(ctx.?));
    n.* += 1;
}

const SeqCtx = struct { seq: *u32, fired_at: *u32 };
fn seqCb(topic: []const u8, data: ?*anyopaque, ctx: ?*anyopaque) void {
    _ = topic;
    _ = data;
    const sc: *SeqCtx = @ptrCast(@alignCast(ctx.?));
    sc.fired_at.* = sc.seq.*;
    sc.seq.* += 1;
}

// --- init ---

test "StaticBroker.init produces a zero-subscriber broker" {
    var broker = T3.init();
    try std.testing.expectEqual(@as(usize, 0), broker.subscriberCount("a"));
    try std.testing.expectEqual(@as(usize, 0), broker.subscriberCount("a/b"));
    try std.testing.expectEqual(@as(usize, 0), broker.subscriberCount("a/b/c"));
}

// --- subscribe / publish basics ---

test "StaticBroker: subscribe and publish — exact match" {
    var broker = T3.init();
    var count: u32 = 0;
    _ = try broker.subscribe("a/b", counterCb, &count);
    broker.publish("a/b", null);
    try std.testing.expectEqual(@as(u32, 1), count);
}

test "StaticBroker: subscribe returns TopicNotFound for unknown topic" {
    var broker = T3.init();
    var count: u32 = 0;
    const result = broker.subscribe("unknown/topic", counterCb, &count);
    try std.testing.expectError(error.TopicNotFound, result);
}

test "StaticBroker: subscribe returns TooManySubscribers when capacity exceeded" {
    // Capacity is 4; fifth subscribe must fail.
    var broker = T3.init();
    var count: u32 = 0;
    _ = try broker.subscribe("a", counterCb, &count);
    _ = try broker.subscribe("a", counterCb, &count);
    _ = try broker.subscribe("a", counterCb, &count);
    _ = try broker.subscribe("a", counterCb, &count);
    const result = broker.subscribe("a", counterCb, &count);
    try std.testing.expectError(error.TooManySubscribers, result);
}

test "StaticBroker: publish is a no-op when no subscribers exist" {
    var broker = T3.init();
    broker.publish("a/b", null); // must not crash
}

test "StaticBroker: publish to unlisted topic is a no-op" {
    var broker = T3.init();
    // "x" is not in T3's topic list — must not crash.
    broker.publish("x", null);
}

test "StaticBroker: multiple subscribers on the same topic all fire" {
    var broker = T3.init();
    var a: u32 = 0;
    var b: u32 = 0;
    _ = try broker.subscribe("a", counterCb, &a);
    _ = try broker.subscribe("a", counterCb, &b);
    broker.publish("a", null);
    try std.testing.expectEqual(@as(u32, 1), a);
    try std.testing.expectEqual(@as(u32, 1), b);
}

test "StaticBroker: delivery order within a topic is LIFO" {
    var broker = T3.init();
    var seq: u32 = 0;
    var fired_at_1: u32 = 999;
    var fired_at_2: u32 = 999;
    var ctx1 = SeqCtx{ .seq = &seq, .fired_at = &fired_at_1 };
    var ctx2 = SeqCtx{ .seq = &seq, .fired_at = &fired_at_2 };

    _ = try broker.subscribe("a", seqCb, &ctx1); // subscribed first  → fires second
    _ = try broker.subscribe("a", seqCb, &ctx2); // subscribed second → fires first

    broker.publish("a", null);
    try std.testing.expectEqual(@as(u32, 0), fired_at_2);
    try std.testing.expectEqual(@as(u32, 1), fired_at_1);
}

// --- hierarchical bubble-up ---

test "StaticBroker: hierarchical: full ancestor chain fires on deep publish" {
    var broker = T3.init();
    var root: u32 = 0;
    var mid: u32 = 0;
    var leaf: u32 = 0;
    _ = try broker.subscribe("a", counterCb, &root);
    _ = try broker.subscribe("a/b", counterCb, &mid);
    _ = try broker.subscribe("a/b/c", counterCb, &leaf);

    broker.publish("a/b/c", null);
    try std.testing.expectEqual(@as(u32, 1), root);
    try std.testing.expectEqual(@as(u32, 1), mid);
    try std.testing.expectEqual(@as(u32, 1), leaf);
}

test "StaticBroker: hierarchical: parent publish does not trickle down to child" {
    var broker = T3.init();
    var count: u32 = 0;
    _ = try broker.subscribe("a/b/c", counterCb, &count);
    broker.publish("a/b", null);
    try std.testing.expectEqual(@as(u32, 0), count);
}

test "StaticBroker: hierarchical: ancestor not in topic list is silently skipped" {
    // "b/c" and "b" are not in T3's list; publishing "b/c" fires nothing.
    var broker = T3.init();
    // We can't subscribe to "b/c" (TopicNotFound), but publish must not crash.
    broker.publish("b/c", null);
}

// --- unsubscribe ---

test "StaticBroker: unsubscribe stops future delivery" {
    var broker = T3.init();
    var count: u32 = 0;
    const h = try broker.subscribe("a", counterCb, &count);
    broker.publish("a", null);
    try std.testing.expectEqual(@as(u32, 1), count);

    broker.unsubscribe(h);
    broker.publish("a", null);
    try std.testing.expectEqual(@as(u32, 1), count); // unchanged
}

test "StaticBroker: unsubscribe with a stale handle is a no-op" {
    var broker = T3.init();
    var count: u32 = 0;
    const h = try broker.subscribe("a", counterCb, &count);
    broker.unsubscribe(h);
    broker.unsubscribe(h); // second call must not crash
}

test "StaticBroker: unsubscribe removes only the targeted subscriber" {
    var broker = T3.init();
    var a: u32 = 0;
    var b: u32 = 0;
    const ha = try broker.subscribe("a", counterCb, &a);
    _ = try broker.subscribe("a", counterCb, &b);

    broker.unsubscribe(ha);
    broker.publish("a", null);
    try std.testing.expectEqual(@as(u32, 0), a); // removed
    try std.testing.expectEqual(@as(u32, 1), b); // untouched
}

test "StaticBroker: LIFO order is preserved after an intermediate unsubscribe" {
    var broker = T3.init();
    var seq: u32 = 0;
    var fired_at_1: u32 = 999;
    var fired_at_2: u32 = 999;
    var fired_at_3: u32 = 999;
    var ctx1 = SeqCtx{ .seq = &seq, .fired_at = &fired_at_1 };
    var ctx2 = SeqCtx{ .seq = &seq, .fired_at = &fired_at_2 };
    var ctx3 = SeqCtx{ .seq = &seq, .fired_at = &fired_at_3 };

    const h1 = try broker.subscribe("a", seqCb, &ctx1);
    _ = try broker.subscribe("a", seqCb, &ctx2);
    _ = try broker.subscribe("a", seqCb, &ctx3);

    broker.unsubscribe(h1); // remove first-registered

    seq = 0;
    broker.publish("a", null);
    try std.testing.expectEqual(@as(u32, 0), fired_at_3); // registered 3rd → fires 1st
    try std.testing.expectEqual(@as(u32, 1), fired_at_2); // registered 2nd → fires 2nd
    try std.testing.expectEqual(@as(u32, 999), fired_at_1); // removed → never fires
}

// --- clearTopic / clearAll ---

test "StaticBroker: clearTopic removes all subscribers for that topic" {
    var broker = T3.init();
    var a: u32 = 0;
    var b: u32 = 0;
    _ = try broker.subscribe("a/b", counterCb, &a);
    _ = try broker.subscribe("a/b", counterCb, &b);

    broker.clearTopic("a/b");
    broker.publish("a/b", null);
    try std.testing.expectEqual(@as(u32, 0), a);
    try std.testing.expectEqual(@as(u32, 0), b);
}

test "StaticBroker: clearTopic on an unknown topic is a no-op" {
    var broker = T3.init();
    broker.clearTopic("not/in/list"); // must not crash
}

test "StaticBroker: clearAll stops delivery on all topics" {
    var broker = T3.init();
    var a: u32 = 0;
    var b: u32 = 0;
    _ = try broker.subscribe("a", counterCb, &a);
    _ = try broker.subscribe("a/b", counterCb, &b);

    broker.clearAll();
    broker.publish("a/b", null); // fires "a/b" and "a" walk; both cleared
    try std.testing.expectEqual(@as(u32, 0), a);
    try std.testing.expectEqual(@as(u32, 0), b);
}

test "StaticBroker: broker is reusable after clearAll" {
    var broker = T3.init();
    var count: u32 = 0;
    _ = try broker.subscribe("a", counterCb, &count);
    broker.clearAll();

    _ = try broker.subscribe("a", counterCb, &count);
    broker.publish("a", null);
    try std.testing.expectEqual(@as(u32, 1), count);
}

// --- subscriberCount ---

test "StaticBroker: subscriberCount returns 0 for unknown topic" {
    var broker = T3.init();
    try std.testing.expectEqual(@as(usize, 0), broker.subscriberCount("unknown"));
}

test "StaticBroker: subscriberCount tracks additions and removals" {
    var broker = T3.init();
    var count: u32 = 0;
    try std.testing.expectEqual(@as(usize, 0), broker.subscriberCount("a"));
    const h1 = try broker.subscribe("a", counterCb, &count);
    try std.testing.expectEqual(@as(usize, 1), broker.subscriberCount("a"));
    _ = try broker.subscribe("a", counterCb, &count);
    try std.testing.expectEqual(@as(usize, 2), broker.subscriberCount("a"));
    broker.unsubscribe(h1);
    try std.testing.expectEqual(@as(usize, 1), broker.subscriberCount("a"));
}

// --- publishExact ---

test "StaticBroker: publishExact fires exact topic only — not ancestors" {
    var broker = T3.init();
    var root: u32 = 0;
    var child: u32 = 0;
    _ = try broker.subscribe("a", counterCb, &root);
    _ = try broker.subscribe("a/b", counterCb, &child);

    broker.publishExact("a/b", null);
    try std.testing.expectEqual(@as(u32, 0), root); // ancestor must NOT fire
    try std.testing.expectEqual(@as(u32, 1), child);
}

test "StaticBroker: publishExact on unknown topic is a no-op" {
    var broker = T3.init();
    broker.publishExact("not/in/list", null);
}

// --- re-entrancy guard ---

test "StaticBroker: publishing flag clears after publish returns" {
    var broker = T3.init();
    var count: u32 = 0;
    _ = try broker.subscribe("a", counterCb, &count);
    broker.publish("a", null);
    broker.publish("a", null); // sequential publish must not trigger the assert
    try std.testing.expectEqual(@as(u32, 2), count);
}

test "StaticBroker: publishing flag is false on a freshly initialised broker" {
    const broker = T3.init();
    try std.testing.expect(!broker.publishing);
}

// --- subscribeOnce ---

test "StaticBroker: subscribeOnce fires the callback exactly once" {
    var broker = T3.init();
    var count: u32 = 0;
    var once: T3.OnceCtx = undefined;
    _ = try broker.subscribeOnce("a", counterCb, &count, &once);

    broker.publish("a", null); // fires once, auto-unsubscribes
    broker.publish("a", null); // no subscriber → no-op
    try std.testing.expectEqual(@as(u32, 1), count);
}

test "StaticBroker: subscribeOnce: early manual cancel prevents delivery" {
    var broker = T3.init();
    var count: u32 = 0;
    var once: T3.OnceCtx = undefined;
    const h = try broker.subscribeOnce("a", counterCb, &count, &once);

    broker.unsubscribe(h);
    broker.publish("a", null);
    try std.testing.expectEqual(@as(u32, 0), count);
}

test "StaticBroker: subscribeOnce: hierarchical bubble-up also triggers auto-unsubscribe" {
    var broker = T3.init();
    var count: u32 = 0;
    var once: T3.OnceCtx = undefined;
    _ = try broker.subscribeOnce("a", counterCb, &count, &once);

    broker.publish("a/b", null); // fires via ancestor walk, then auto-unsubscribes
    broker.publish("a/b", null);
    try std.testing.expectEqual(@as(u32, 1), count);
}

// --- zero-capacity and zero-topic edge cases ---

test "StaticBroker: zero max_subscribers — every subscribe returns TooManySubscribers" {
    const Empty = StaticBroker(&.{"t"}, 0);
    var broker = Empty.init();
    var count: u32 = 0;
    try std.testing.expectError(error.TooManySubscribers, broker.subscribe("t", counterCb, &count));
}

test "StaticBroker: zero topics — every subscribe returns TopicNotFound" {
    const Bare = StaticBroker(&.{}, 4);
    var broker = Bare.init();
    var count: u32 = 0;
    try std.testing.expectError(error.TopicNotFound, broker.subscribe("any", counterCb, &count));
    broker.publish("any", null); // must not crash
    broker.clearAll(); // must not crash
}
