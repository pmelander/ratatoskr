//! sync_broker — mutual-exclusion wrapper for any ratatoskr broker type.
//!
//! ## Design note
//!
//! ### What it does and why it belongs here
//!
//! `LockedBroker(Inner, Lock)` is a comptime wrapper that adds mutual exclusion
//! to any existing broker (`Broker`, `StaticBroker`, `TypedBroker`).  All public
//! methods are proxied behind a caller-provided lock, making the broker safe to
//! use from multiple OS threads or RTOS tasks.
//!
//! The `Lock` type is a comptime parameter so that:
//!   - Hosted targets can use `std.Thread.Mutex` (or any OS primitive).
//!   - Bare-metal / freestanding targets can use the bundled `SpinLock`
//!     (a single atomic bool; no OS dependency).
//!   - Single-threaded callers pay zero overhead by passing a `NullLock`
//!     (though they should just use the inner broker directly).
//!
//! ### Dispatch model — lock held for entire publish
//!
//! The lock is acquired at the start of `publish`/`publishExact` and released
//! only after all callbacks have returned.  This is the correct choice for
//! embedded systems where callbacks must be lightweight and non-blocking, and
//! it keeps the implementation allocation-free (no subscriber-list snapshot is
//! needed).
//!
//! **Consequence**: a callback must NOT call back into the same `LockedBroker`
//! from within its own invocation — that would attempt to re-acquire the lock
//! and deadlock.  For callback-to-broker re-entry across threads, use
//! `EventQueue` to post events and drain them from the consumer thread instead.
//!
//! **`subscribeOnce` exception**: the internal `onceFn` shim calls
//! `inner.unsubscribe` (on the *unwrapped* broker), not `LockedBroker.unsubscribe`,
//! so it does not re-acquire the lock.  `subscribeOnce` is therefore safe to
//! use through a `LockedBroker`.
//!
//! ### Public types and function signatures
//!
//! ```
//! pub const SpinLock = struct { ... }
//!   pub fn init() SpinLock
//!   pub fn lock(self: *SpinLock) void       // busy-waits
//!   pub fn unlock(self: *SpinLock) void
//!
//! pub fn LockedBroker(comptime Inner: type, comptime Lock: type) type
//!   // Returned type:
//!   pub fn init(inner: Inner, lock_impl: Lock) Self
//!   pub fn deinit(self: *Self) void           // no-op if Inner has no deinit
//!   pub fn subscribe(topic, callback, ctx) !Handle
//!   pub fn unsubscribe(handle) void
//!   pub fn publish(topic, data) void
//!   pub fn publishExact(topic, data) void
//!   pub fn clearTopic(topic) void
//!   pub fn clearAll() void
//!   pub fn subscriberCount(topic) usize
//!   // subscribeOnce and OnceCtx forwarded when Inner exposes them
//!   // topicIterator forwarded when Inner exposes it (lock NOT held during iteration)
//! ```
//!
//! ### Allocator strategy
//!
//! None in the wrapper itself.  Memory is managed entirely by `Inner`.
//!
//! ### Comptime constraints and platform gates
//!
//! - `Lock` is duck-typed: must expose `fn lock(*Lock) void` and
//!   `fn unlock(*Lock) void`; a `@compileError` is issued otherwise.
//! - `SpinLock` uses `std.atomic.Value(bool)` — compiles for freestanding.
//! - No `std.os` calls; no allocator; safe for any target.

const std = @import("std");
const pubsub = @import("pubsub.zig");

pub const Handle = pubsub.Handle;

// ---------------------------------------------------------------------------
// SpinLock
// ---------------------------------------------------------------------------

/// A freestanding-safe spinlock backed by a single atomic boolean.
///
/// Busy-waits on contention — suitable for bare-metal / RTOS contexts where
/// `std.Thread.Mutex` is unavailable.  On hosted targets, prefer
/// `std.Thread.Mutex` to avoid wasting CPU cycles.
///
/// Usage:
///   var mu = SpinLock.init();
///   mu.lock();
///   defer mu.unlock();
pub const SpinLock = struct {
    /// `true` while the lock is held; `false` when free.
    flag: std.atomic.Value(bool),

    /// Create an unlocked SpinLock.
    pub fn init() SpinLock {
        return .{ .flag = std.atomic.Value(bool).init(false) };
    }

    /// Acquire the lock.
    ///
    /// Spins until a CAS from `false` → `true` succeeds.
    /// `acquire` ordering ensures all memory writes by the previous holder
    /// are visible to this thread after `lock` returns.
    pub fn lock(self: *SpinLock) void {
        while (self.flag.cmpxchgWeak(false, true, .acquire, .monotonic) != null) {
            std.atomic.spinLoopHint();
        }
    }

    /// Release the lock.
    ///
    /// `release` ordering ensures all memory writes done while holding the
    /// lock are visible to the next thread that acquires it.
    pub fn unlock(self: *SpinLock) void {
        self.flag.store(false, .release);
    }
};

// ---------------------------------------------------------------------------
// LockedBroker
// ---------------------------------------------------------------------------

/// Wraps any broker type `Inner` with mutual exclusion provided by `Lock`.
///
/// `Lock` must satisfy the following interface (duck-typed at comptime):
///   `fn lock(self: *Lock) void`
///   `fn unlock(self: *Lock) void`
///
/// The lock is held for the full duration of every public method, including
/// callback dispatch inside `publish`/`publishExact`.  Callbacks must not
/// call back into the same `LockedBroker` — see module doc for details.
///
/// Example — hosted target with std.Thread.Mutex:
///
///   const SyncBroker = ratatoskr.LockedBroker(ratatoskr.Broker, std.Thread.Mutex);
///   var b = SyncBroker.init(ratatoskr.Broker.init(allocator), .{});
///   defer b.deinit();
///
/// Example — bare-metal with SpinLock and StaticBroker:
///
///   const Events = ratatoskr.StaticBroker(&.{ "sensors/temp" }, 4);
///   const SyncEvents = ratatoskr.LockedBroker(Events, ratatoskr.SpinLock);
///   var b = SyncEvents.init(Events.init(), ratatoskr.SpinLock.init());
pub fn LockedBroker(comptime Inner: type, comptime Lock: type) type {
    // Duck-type check: Lock must expose lock() and unlock().
    comptime {
        if (!@hasDecl(Lock, "lock"))
            @compileError("LockedBroker: Lock must have a `lock` method");
        if (!@hasDecl(Lock, "unlock"))
            @compileError("LockedBroker: Lock must have an `unlock` method");
    }

    return struct {
        const Self = @This();

        /// The wrapped broker.  Do not access directly from multiple threads.
        inner: Inner,
        /// The lock that serialises all access to `inner`.
        lock_impl: Lock,

        // ---- Conditional declarations (lazy analysis) -----------------------
        //
        // `usingnamespace` was removed in Zig 0.16.  In its place we rely on
        // Zig's lazy declaration analysis: declarations inside a struct are only
        // compiled when they are actually referenced.  If `Inner` does not expose
        // `OnceCtx`, `subscribeOnce`, or `topicIterator`, accessing the
        // corresponding `LockedBroker` declaration simply produces a clear
        // compile error at the call site — identical behaviour to the old
        // conditional `usingnamespace` mixin.

        /// Storage type for once-subscriptions.
        ///
        /// Available when `Inner` exposes `OnceCtx` (Broker and StaticBroker).
        /// Accessing this declaration when Inner does not have `OnceCtx` is a
        /// compile error.
        pub const OnceCtx = Inner.OnceCtx;

        /// Acquire the lock, register a once-subscription on `inner`, release.
        ///
        /// Available when `Inner` exposes `subscribeOnce` (Broker and
        /// StaticBroker).  Calling this when Inner does not support it is a
        /// compile error.
        ///
        /// `once_buf` must remain valid until the callback fires (or until
        /// `unsubscribe(returned_handle)` is called to cancel early).
        /// See `Inner.subscribeOnce` for full semantics.
        ///
        /// Safety: the `onceFn` shim inside Inner calls `inner.unsubscribe`
        /// directly (not LockedBroker.unsubscribe), so it does not re-acquire
        /// the lock even though it runs from within `publish` while the lock is
        /// held.  This is intentional and safe.
        pub fn subscribeOnce(
            self: *Self,
            topic: []const u8,
            callback: anytype,
            ctx: ?*anyopaque,
            once_buf: *OnceCtx,
        ) !Handle {
            self.lock_impl.lock();
            defer self.lock_impl.unlock();
            return self.inner.subscribeOnce(topic, callback, ctx, once_buf);
        }

        /// Return an iterator over currently subscribed topic strings.
        ///
        /// Available when `Inner` exposes `topicIterator` (Broker only;
        /// StaticBroker and TypedBroker do not).  Calling this when Inner does
        /// not support it is a compile error.
        ///
        /// WARNING: the lock is NOT held during iteration.  The iterator
        /// borrows from `inner`; callers must not call subscribe, unsubscribe,
        /// clearTopic, or clearAll while the iterator is live.  For a
        /// consistent snapshot under concurrent access, acquire `lock_impl`
        /// externally for the duration of the iteration.
        pub fn topicIterator(self: *Self) @typeInfo(@TypeOf(Inner.topicIterator)).@"fn".return_type.? {
            return self.inner.topicIterator();
        }

        // ---- Declarations ---------------------------------------------------

        /// Initialise by wrapping a pre-created `inner` broker with `lock_impl`.
        ///
        /// The caller is responsible for initialising `inner` before passing it
        /// here, because different broker types have different `init` signatures.
        ///
        /// Example:
        ///   LockedBroker(Broker, SpinLock).init(Broker.init(alloc), SpinLock.init())
        pub fn init(inner: Inner, lock_impl: Lock) Self {
            return .{ .inner = inner, .lock_impl = lock_impl };
        }

        /// Release all resources owned by the inner broker.
        ///
        /// A no-op wrapper is generated at comptime when `Inner` has no `deinit`
        /// (e.g. `StaticBroker`).
        pub fn deinit(self: *Self) void {
            if (comptime @hasDecl(Inner, "deinit")) {
                self.inner.deinit();
            }
        }

        /// Acquire the lock, delegate `subscribe` to `inner`, release the lock.
        ///
        /// The `callback` type is inferred so that both raw (`pubsub.CallbackFn`)
        /// and typed (`TypedBroker.CallbackFn`) signatures are accepted without
        /// casting at the call site.
        ///
        /// Errors: propagated from `Inner.subscribe` unchanged (e.g. `OutOfMemory`,
        ///         `TopicNotFound`, `TooManySubscribers`).
        pub fn subscribe(
            self: *Self,
            topic: []const u8,
            callback: anytype,
            ctx: ?*anyopaque,
        ) !Handle {
            self.lock_impl.lock();
            defer self.lock_impl.unlock();
            return self.inner.subscribe(topic, callback, ctx);
        }

        /// Acquire the lock, delegate `unsubscribe` to `inner`, release the lock.
        pub fn unsubscribe(self: *Self, handle: Handle) void {
            self.lock_impl.lock();
            defer self.lock_impl.unlock();
            self.inner.unsubscribe(handle);
        }

        /// Acquire the lock, dispatch `publish` through `inner` (including all
        /// callbacks), then release the lock.
        ///
        /// `data` is forwarded verbatim — `?*anyopaque` for raw brokers,
        /// typed pointer for `TypedBroker`.  Callbacks fire while the lock is
        /// held; they must not call back into this `LockedBroker`.
        pub fn publish(self: *Self, topic: []const u8, data: anytype) void {
            self.lock_impl.lock();
            defer self.lock_impl.unlock();
            self.inner.publish(topic, data);
        }

        /// Acquire the lock, dispatch `publishExact` through `inner`, release.
        pub fn publishExact(self: *Self, topic: []const u8, data: anytype) void {
            self.lock_impl.lock();
            defer self.lock_impl.unlock();
            self.inner.publishExact(topic, data);
        }

        /// Acquire the lock, clear the topic, release the lock.
        pub fn clearTopic(self: *Self, topic: []const u8) void {
            self.lock_impl.lock();
            defer self.lock_impl.unlock();
            self.inner.clearTopic(topic);
        }

        /// Acquire the lock, clear all subscriptions, release the lock.
        pub fn clearAll(self: *Self) void {
            self.lock_impl.lock();
            defer self.lock_impl.unlock();
            self.inner.clearAll();
        }

        /// Acquire the lock, read the subscriber count, release the lock.
        pub fn subscriberCount(self: *Self, topic: []const u8) usize {
            self.lock_impl.lock();
            defer self.lock_impl.unlock();
            return self.inner.subscriberCount(topic);
        }
    };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// Minimal smoke tests — full behavioural coverage lives in pubsub.zig and
// static_broker.zig.  These tests verify that the wrapper compiles, routes
// calls correctly, and that SpinLock is coherent.

const Cb = struct {
    fn counter(topic: []const u8, data: ?*anyopaque, ctx: ?*anyopaque) void {
        _ = topic;
        _ = data;
        const n: *u32 = @ptrCast(@alignCast(ctx.?));
        n.* += 1;
    }
};

// --- SpinLock ---

test "SpinLock.init produces an unlocked lock" {
    const mu = SpinLock.init();
    try std.testing.expect(!mu.flag.load(.acquire));
}

test "SpinLock: lock then unlock leaves lock free" {
    var mu = SpinLock.init();
    mu.lock();
    try std.testing.expect(mu.flag.load(.acquire)); // held
    mu.unlock();
    try std.testing.expect(!mu.flag.load(.acquire)); // free
}

// --- LockedBroker wrapping Broker ---

test "LockedBroker(Broker, SpinLock): subscribe and publish reach inner broker" {
    const Broker = pubsub.Broker(.{});
    const LB = LockedBroker(Broker, SpinLock);

    var lb = LB.init(Broker.init(std.testing.allocator), SpinLock.init());
    defer lb.deinit();

    var count: u32 = 0;
    _ = try lb.subscribe("t", Cb.counter, &count);
    lb.publish("t", @as(?*anyopaque, null));
    try std.testing.expectEqual(@as(u32, 1), count);
}

test "LockedBroker(Broker, SpinLock): unsubscribe stops delivery" {
    const Broker = pubsub.Broker(.{});
    const LB = LockedBroker(Broker, SpinLock);

    var lb = LB.init(Broker.init(std.testing.allocator), SpinLock.init());
    defer lb.deinit();

    var count: u32 = 0;
    const h = try lb.subscribe("t", Cb.counter, &count);
    lb.publish("t", @as(?*anyopaque, null));
    try std.testing.expectEqual(@as(u32, 1), count);

    lb.unsubscribe(h);
    lb.publish("t", @as(?*anyopaque, null));
    try std.testing.expectEqual(@as(u32, 1), count); // unchanged
}

test "LockedBroker(Broker, SpinLock): publishExact does not fire ancestors" {
    const Broker = pubsub.Broker(.{});
    const LB = LockedBroker(Broker, SpinLock);

    var lb = LB.init(Broker.init(std.testing.allocator), SpinLock.init());
    defer lb.deinit();

    var root: u32 = 0;
    var child: u32 = 0;
    _ = try lb.subscribe("a", Cb.counter, &root);
    _ = try lb.subscribe("a/b", Cb.counter, &child);

    lb.publishExact("a/b", @as(?*anyopaque, null));
    try std.testing.expectEqual(@as(u32, 0), root);
    try std.testing.expectEqual(@as(u32, 1), child);
}

test "LockedBroker(Broker, SpinLock): clearTopic stops delivery" {
    const Broker = pubsub.Broker(.{});
    const LB = LockedBroker(Broker, SpinLock);

    var lb = LB.init(Broker.init(std.testing.allocator), SpinLock.init());
    defer lb.deinit();

    var count: u32 = 0;
    _ = try lb.subscribe("t", Cb.counter, &count);
    lb.clearTopic("t");
    lb.publish("t", @as(?*anyopaque, null));
    try std.testing.expectEqual(@as(u32, 0), count);
}

test "LockedBroker(Broker, SpinLock): clearAll stops delivery on all topics" {
    const Broker = pubsub.Broker(.{});
    const LB = LockedBroker(Broker, SpinLock);

    var lb = LB.init(Broker.init(std.testing.allocator), SpinLock.init());
    defer lb.deinit();

    var a: u32 = 0;
    var b: u32 = 0;
    _ = try lb.subscribe("x", Cb.counter, &a);
    _ = try lb.subscribe("y", Cb.counter, &b);
    lb.clearAll();
    lb.publish("x", @as(?*anyopaque, null));
    lb.publish("y", @as(?*anyopaque, null));
    try std.testing.expectEqual(@as(u32, 0), a);
    try std.testing.expectEqual(@as(u32, 0), b);
}

test "LockedBroker(Broker, SpinLock): subscriberCount reflects live subscriptions" {
    const Broker = pubsub.Broker(.{});
    const LB = LockedBroker(Broker, SpinLock);

    var lb = LB.init(Broker.init(std.testing.allocator), SpinLock.init());
    defer lb.deinit();

    var count: u32 = 0;
    try std.testing.expectEqual(@as(usize, 0), lb.subscriberCount("t"));
    const h = try lb.subscribe("t", Cb.counter, &count);
    try std.testing.expectEqual(@as(usize, 1), lb.subscriberCount("t"));
    lb.unsubscribe(h);
    try std.testing.expectEqual(@as(usize, 0), lb.subscriberCount("t"));
}

test "LockedBroker(Broker, SpinLock): subscribeOnce fires once then stops" {
    const Broker = pubsub.Broker(.{});
    const LB = LockedBroker(Broker, SpinLock);

    var lb = LB.init(Broker.init(std.testing.allocator), SpinLock.init());
    defer lb.deinit();

    var count: u32 = 0;
    var once: LB.OnceCtx = undefined;
    _ = try lb.subscribeOnce("t", Cb.counter, &count, &once);

    lb.publish("t", @as(?*anyopaque, null));
    lb.publish("t", @as(?*anyopaque, null)); // no subscriber
    try std.testing.expectEqual(@as(u32, 1), count);
}

test "LockedBroker(Broker, SpinLock): topicIterator yields subscribed topics" {
    const Broker = pubsub.Broker(.{});
    const LB = LockedBroker(Broker, SpinLock);

    var lb = LB.init(Broker.init(std.testing.allocator), SpinLock.init());
    defer lb.deinit();

    var count: u32 = 0;
    _ = try lb.subscribe("alpha", Cb.counter, &count);
    _ = try lb.subscribe("beta", Cb.counter, &count);

    var seen_alpha = false;
    var seen_beta = false;
    var it = lb.topicIterator();
    while (it.next()) |t| {
        if (std.mem.eql(u8, t.*, "alpha")) seen_alpha = true;
        if (std.mem.eql(u8, t.*, "beta")) seen_beta = true;
    }
    try std.testing.expect(seen_alpha);
    try std.testing.expect(seen_beta);
}

// --- LockedBroker wrapping StaticBroker ---

test "LockedBroker(StaticBroker, SpinLock): subscribe and publish reach inner broker" {
    const static_broker = @import("static_broker.zig");
    const SB = static_broker.StaticBroker(&.{"evt"}, 4);
    const LB = LockedBroker(SB, SpinLock);

    // StaticBroker has no deinit — LockedBroker.deinit is a no-op.
    var lb = LB.init(SB.init(), SpinLock.init());

    var count: u32 = 0;
    _ = try lb.subscribe("evt", Cb.counter, &count);
    lb.publish("evt", @as(?*anyopaque, null));
    try std.testing.expectEqual(@as(u32, 1), count);
}

test "LockedBroker(StaticBroker, SpinLock): subscribeOnce available and works" {
    const static_broker = @import("static_broker.zig");
    const SB = static_broker.StaticBroker(&.{"s"}, 4);
    const LB = LockedBroker(SB, SpinLock);

    var lb = LB.init(SB.init(), SpinLock.init());

    var count: u32 = 0;
    var once: LB.OnceCtx = undefined;
    _ = try lb.subscribeOnce("s", Cb.counter, &count, &once);
    lb.publish("s", @as(?*anyopaque, null));
    lb.publish("s", @as(?*anyopaque, null));
    try std.testing.expectEqual(@as(u32, 1), count);
}

// --- LockedBroker with an external lock type (compile-time check only) ---
//
// std.Thread.Mutex was removed in Zig 0.16.  Use std.debug.SafetyLock as a
// representative external lock: it has the required `lock(self)` /
// `unlock(self)` interface and compiles on all targets.

test "LockedBroker(Broker, std.debug.SafetyLock): compiles and basic ops work" {
    const Broker = pubsub.Broker(.{});
    const LB = LockedBroker(Broker, std.debug.SafetyLock);

    var lb = LB.init(Broker.init(std.testing.allocator), .{});
    defer lb.deinit();

    var count: u32 = 0;
    _ = try lb.subscribe("m", Cb.counter, &count);
    lb.publish("m", @as(?*anyopaque, null));
    try std.testing.expectEqual(@as(u32, 1), count);
}
