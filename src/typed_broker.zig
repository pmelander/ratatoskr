//! typed_broker — comptime type-safe wrapper around `Broker`.
//!
//! `TypedBroker(Payload)` eliminates `?*anyopaque` casting in user callbacks
//! by generating strongly-typed publish/subscribe surfaces at comptime.
//!
//! ### Payload conventions
//!
//!   - `Payload = void`: signal-style broker; `publish` takes no data argument
//!     and callbacks receive `void`.
//!   - Any other type: `publish` accepts `*const Payload`; callbacks receive
//!     `*const Payload`.  The pointer is valid **only for the duration of the
//!     callback**.  Copy the value out if you need it to outlive the call.
//!
//! ### Overhead vs raw `Broker`
//!
//! Each `subscribe` call allocates one `WrapCtx` record (two pointer-sized
//! fields) and one entry in a tracking `AutoHashMap`.  Callers that need
//! zero-per-subscription overhead should use `Broker` directly.
//!
//! ### Thread safety / re-entrancy
//!
//! Same rules as `Broker`: not thread-safe, and re-entrant `publish`/
//! `publishExact` from within a callback panics in debug and ReleaseSafe builds.

const std = @import("std");
const pubsub = @import("pubsub.zig");

pub const Handle = pubsub.Handle;
pub const TOPIC_SEPARATOR = pubsub.TOPIC_SEPARATOR;

/// Returns a type-safe PubSub broker specialised for `Payload`.
///
/// Example:
///
///   const SensorBroker = TypedBroker(SensorReading);
///   var broker = SensorBroker.init(allocator);
///   defer broker.deinit();
///
///   const h = try broker.subscribe("sensors/temp", onReading, &state);
///   var reading = SensorReading{ .temp = 36.6 };
///   broker.publish("sensors/temp", &reading);
///   broker.unsubscribe(h);
pub fn TypedBroker(comptime Payload: type) type {
    return struct {
        const Self = @This();

        /// Typed callback signature.
        ///
        /// When `Payload == void`, `data` is `void` (no pointer, no cast needed).
        /// Otherwise `data` is `*const Payload` — valid for the duration of this
        /// invocation only.
        pub const CallbackFn = *const fn (
            topic: []const u8,
            data: if (Payload == void) void else *const Payload,
            ctx: ?*anyopaque,
        ) void;

        /// Internal bridge record: typed callback + caller's original context.
        const WrapCtx = struct {
            typed_cb: CallbackFn,
            user_ctx: ?*anyopaque,
        };

        /// Entry in the tracking map; carries the borrowed topic slice so that
        /// `clearTopic` can find and free WrapCtx objects without a separate map.
        const WrapEntry = struct {
            wc: *WrapCtx,
            /// Same borrowed slice as Handle.topic — not owned.
            topic: []const u8,
        };

        /// Raw shim registered with the inner Broker.
        /// Casts the raw ctx back to `*WrapCtx` and forwards to the typed callback.
        fn shim(topic: []const u8, data: ?*anyopaque, ctx: ?*anyopaque) void {
            const wc: *WrapCtx = @ptrCast(@alignCast(ctx.?));
            if (Payload == void) {
                wc.typed_cb(topic, {}, wc.user_ctx);
            } else {
                const payload: *const Payload = @ptrCast(@alignCast(data.?));
                wc.typed_cb(topic, payload, wc.user_ctx);
            }
        }

        inner: pubsub.Broker(.{}),
        /// Tracks WrapCtx allocations so they can be freed on unsubscribe/deinit.
        wraps: std.AutoHashMap(u64, WrapEntry),

        /// Initialise a TypedBroker backed by `allocator`.
        pub fn init(allocator: std.mem.Allocator) Self {
            return .{
                .inner = pubsub.Broker(.{}).init(allocator),
                .wraps = std.AutoHashMap(u64, WrapEntry).init(allocator),
            };
        }

        /// Release all resources.  Any outstanding Handles are invalidated.
        pub fn deinit(self: *Self) void {
            var it = self.wraps.valueIterator();
            while (it.next()) |entry| {
                self.inner.allocator.destroy(entry.wc);
            }
            self.wraps.deinit();
            self.inner.deinit();
        }

        /// Register `callback`+`ctx` for `topic`.
        ///
        /// Returns a Handle to cancel the subscription.
        /// `topic` memory must outlive the Handle (the slice is not copied).
        ///
        /// Errors: `error.OutOfMemory` — broker state is unchanged on failure.
        pub fn subscribe(
            self: *Self,
            topic: []const u8,
            callback: CallbackFn,
            ctx: ?*anyopaque,
        ) error{OutOfMemory}!Handle {
            // 1. Allocate the bridge record.
            const wc = try self.inner.allocator.create(WrapCtx);
            wc.* = .{ .typed_cb = callback, .user_ctx = ctx };
            errdefer self.inner.allocator.destroy(wc);

            // 2. Register the shim with the raw broker.
            const handle = try self.inner.subscribe(topic, shim, @ptrCast(wc));
            errdefer self.inner.unsubscribe(handle);

            // 3. Track the allocation for future cleanup.
            try self.wraps.put(handle.id, .{ .wc = wc, .topic = handle.topic });
            return handle;
        }

        /// Cancel the subscription identified by `handle`.
        /// Safe to call with a stale or already-unsubscribed handle (no-op).
        pub fn unsubscribe(self: *Self, handle: Handle) void {
            self.inner.unsubscribe(handle);
            if (self.wraps.fetchRemove(handle.id)) |kv| {
                self.inner.allocator.destroy(kv.value.wc);
            }
        }

        /// Remove ALL subscriptions for `topic`, freeing their WrapCtx records.
        ///
        /// Allocation-free: rescans the tracking map after each individual removal
        /// rather than collecting IDs into a temporary buffer, since HashMap
        /// iterators are invalidated by structural modifications.
        pub fn clearTopic(self: *Self, topic: []const u8) void {
            var found = true;
            while (found) {
                found = false;
                var it = self.wraps.iterator();
                while (it.next()) |entry| {
                    if (std.mem.eql(u8, entry.value_ptr.topic, topic)) {
                        const id = entry.key_ptr.*;
                        self.inner.allocator.destroy(entry.value_ptr.wc);
                        _ = self.wraps.remove(id);
                        found = true;
                        break; // restart after modifying the map
                    }
                }
            }
            self.inner.clearTopic(topic);
        }

        /// Deliver to all subscribers of `topic` and every ancestor topic (bubble-up).
        ///
        /// When `Payload == void`, call as `broker.publish(topic, {})`.
        /// Otherwise pass a `*const Payload`; the pointer is valid for the
        /// duration of all callbacks triggered by this call.
        pub fn publish(
            self: *Self,
            topic: []const u8,
            data: if (Payload == void) void else *const Payload,
        ) void {
            if (Payload == void) {
                self.inner.publish(topic, null);
            } else {
                self.inner.publish(topic, @ptrCast(@constCast(data)));
            }
        }

        /// Deliver to subscribers of `topic` only — no ancestor walk.
        ///
        /// See `Broker.publishExact` for full semantics.
        pub fn publishExact(
            self: *Self,
            topic: []const u8,
            data: if (Payload == void) void else *const Payload,
        ) void {
            if (Payload == void) {
                self.inner.publishExact(topic, null);
            } else {
                self.inner.publishExact(topic, @ptrCast(@constCast(data)));
            }
        }
    };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "TypedBroker(u32): typed callback receives correct value" {
    const T = TypedBroker(u32);
    var broker = T.init(std.testing.allocator);
    defer broker.deinit();

    const Cb = struct {
        fn cb(topic: []const u8, data: *const u32, ctx: ?*anyopaque) void {
            _ = topic;
            const out: *u32 = @ptrCast(@alignCast(ctx.?));
            out.* = data.*;
        }
    };

    var received: u32 = 0;
    _ = try broker.subscribe("t", Cb.cb, &received);

    var val: u32 = 42;
    broker.publish("t", &val);
    try std.testing.expectEqual(@as(u32, 42), received);
}

test "TypedBroker(void): signal-style publish increments counter" {
    const T = TypedBroker(void);
    var broker = T.init(std.testing.allocator);
    defer broker.deinit();

    const Cb = struct {
        fn cb(topic: []const u8, data: void, ctx: ?*anyopaque) void {
            _ = topic;
            _ = data;
            const n: *u32 = @ptrCast(@alignCast(ctx.?));
            n.* += 1;
        }
    };

    var count: u32 = 0;
    _ = try broker.subscribe("sig", Cb.cb, &count);
    broker.publish("sig", {});
    try std.testing.expectEqual(@as(u32, 1), count);
}

test "TypedBroker: struct payload is forwarded correctly" {
    const Point = struct { x: i32, y: i32 };
    const T = TypedBroker(Point);
    var broker = T.init(std.testing.allocator);
    defer broker.deinit();

    const Cb = struct {
        fn cb(topic: []const u8, data: *const Point, ctx: ?*anyopaque) void {
            _ = topic;
            const out: *Point = @ptrCast(@alignCast(ctx.?));
            out.* = data.*;
        }
    };

    var got = Point{ .x = 0, .y = 0 };
    _ = try broker.subscribe("pos", Cb.cb, &got);

    var p = Point{ .x = 3, .y = 7 };
    broker.publish("pos", &p);
    try std.testing.expectEqual(@as(i32, 3), got.x);
    try std.testing.expectEqual(@as(i32, 7), got.y);
}

test "TypedBroker: hierarchical bubble-up still fires ancestors" {
    const T = TypedBroker(u32);
    var broker = T.init(std.testing.allocator);
    defer broker.deinit();

    const Cb = struct {
        fn cb(topic: []const u8, data: *const u32, ctx: ?*anyopaque) void {
            _ = topic;
            _ = data;
            const n: *u32 = @ptrCast(@alignCast(ctx.?));
            n.* += 1;
        }
    };

    var root: u32 = 0;
    var child: u32 = 0;
    _ = try broker.subscribe("a", Cb.cb, &root);
    _ = try broker.subscribe("a/b", Cb.cb, &child);

    var val: u32 = 1;
    broker.publish("a/b", &val);
    try std.testing.expectEqual(@as(u32, 1), root);
    try std.testing.expectEqual(@as(u32, 1), child);
}

test "TypedBroker: unsubscribe stops delivery" {
    const T = TypedBroker(u32);
    var broker = T.init(std.testing.allocator);
    defer broker.deinit();

    const Cb = struct {
        fn cb(topic: []const u8, data: *const u32, ctx: ?*anyopaque) void {
            _ = topic;
            _ = data;
            const n: *u32 = @ptrCast(@alignCast(ctx.?));
            n.* += 1;
        }
    };

    var count: u32 = 0;
    const h = try broker.subscribe("t", Cb.cb, &count);
    var val: u32 = 0;
    broker.publish("t", &val);
    try std.testing.expectEqual(@as(u32, 1), count);

    broker.unsubscribe(h);
    broker.publish("t", &val);
    try std.testing.expectEqual(@as(u32, 1), count); // unchanged
}

test "TypedBroker: clearTopic removes all subscribers" {
    const T = TypedBroker(u32);
    var broker = T.init(std.testing.allocator);
    defer broker.deinit();

    const Cb = struct {
        fn cb(topic: []const u8, data: *const u32, ctx: ?*anyopaque) void {
            _ = topic;
            _ = data;
            const n: *u32 = @ptrCast(@alignCast(ctx.?));
            n.* += 1;
        }
    };

    var a: u32 = 0;
    var b: u32 = 0;
    _ = try broker.subscribe("ch", Cb.cb, &a);
    _ = try broker.subscribe("ch", Cb.cb, &b);

    broker.clearTopic("ch");
    var val: u32 = 0;
    broker.publish("ch", &val);
    try std.testing.expectEqual(@as(u32, 0), a);
    try std.testing.expectEqual(@as(u32, 0), b);
}

test "TypedBroker: deinit with active subscriptions does not leak" {
    const T = TypedBroker(u32);
    var broker = T.init(std.testing.allocator);
    const Cb = struct {
        fn cb(topic: []const u8, data: *const u32, ctx: ?*anyopaque) void {
            _ = topic;
            _ = data;
            _ = ctx;
        }
    };
    var val: u32 = 0;
    _ = try broker.subscribe("a", Cb.cb, &val);
    _ = try broker.subscribe("a", Cb.cb, &val);
    broker.deinit(); // testing.allocator will catch leaks
}

test "TypedBroker: publishExact does not fire ancestors" {
    const T = TypedBroker(u32);
    var broker = T.init(std.testing.allocator);
    defer broker.deinit();

    const Cb = struct {
        fn cb(topic: []const u8, data: *const u32, ctx: ?*anyopaque) void {
            _ = topic;
            _ = data;
            const n: *u32 = @ptrCast(@alignCast(ctx.?));
            n.* += 1;
        }
    };

    var root: u32 = 0;
    var child: u32 = 0;
    _ = try broker.subscribe("a", Cb.cb, &root);
    _ = try broker.subscribe("a/b", Cb.cb, &child);

    var val: u32 = 0;
    broker.publishExact("a/b", &val);
    try std.testing.expectEqual(@as(u32, 0), root); // ancestor must NOT fire
    try std.testing.expectEqual(@as(u32, 1), child);
}

test "TypedBroker(void): publishExact signal does not fire ancestors" {
    const T = TypedBroker(void);
    var broker = T.init(std.testing.allocator);
    defer broker.deinit();

    const Cb = struct {
        fn cb(topic: []const u8, data: void, ctx: ?*anyopaque) void {
            _ = topic;
            _ = data;
            const n: *u32 = @ptrCast(@alignCast(ctx.?));
            n.* += 1;
        }
    };

    var root: u32 = 0;
    var child: u32 = 0;
    _ = try broker.subscribe("a", Cb.cb, &root);
    _ = try broker.subscribe("a/b", Cb.cb, &child);

    broker.publishExact("a/b", {});
    try std.testing.expectEqual(@as(u32, 0), root);
    try std.testing.expectEqual(@as(u32, 1), child);
}

test "TypedBroker: subscribe is OOM-safe at every allocation point" {
    const T = TypedBroker(u32);
    const Cb = struct {
        fn cb(topic: []const u8, data: *const u32, ctx: ?*anyopaque) void {
            _ = topic;
            _ = data;
            _ = ctx;
        }
    };

    var fail_index: usize = 0;
    while (true) : (fail_index += 1) {
        var fa = std.testing.FailingAllocator.init(
            std.testing.allocator,
            .{ .fail_index = fail_index },
        );
        var broker = T.init(fa.allocator());
        var dummy: u32 = 0;
        if (broker.subscribe("x", Cb.cb, &dummy)) |_| {
            broker.deinit();
            break;
        } else |_| {
            broker.deinit(); // must not crash or leak on any OOM path
        }
    }
}
