//! event_queue — MPSC lock-free ring buffer for cross-thread broker dispatch.
//!
//! ## Design note
//!
//! ### What it does and why it belongs here
//!
//! `EventQueue` solves the cross-thread delivery problem that `LockedBroker`
//! cannot: getting a callback to execute in a different thread than the one
//! that called `publish`.
//!
//! The classic embedded pattern it targets:
//!
//!   ISR / producer task           Consumer / event-loop task
//!   ─────────────────────         ──────────────────────────
//!   eq.tryPost("sensors/t", &v)   loop {
//!                                   eq.drain(&broker);
//!                                   // callbacks fire here, in this thread
//!                                 }
//!
//! `tryPost` is intentionally non-blocking (returns `error.QueueFull` rather
//! than spinning) so it is safe to call from an interrupt service routine.
//! `drain` is called from the consumer thread and is the only place where
//! broker callbacks are invoked — no cross-thread callback surprise.
//!
//! ### Concurrency model
//!
//! MPSC — Multiple Producer, Single Consumer:
//!   - Multiple threads / ISRs may call `tryPost` concurrently.  A `Lock`
//!     parameter serialises them; use `SpinLock` (from `sync_broker.zig`) for
//!     bare-metal or `std.Thread.Mutex` for hosted targets.
//!   - Exactly ONE thread calls `drain`.  No lock is required on the consumer
//!     side; atomic acquire/release ordering on the head index provides
//!     visibility.
//!
//! For single-producer use, pass `NullLock` — it compiles away entirely.
//!
//! ### Internal layout and memory ordering
//!
//!   head  (atomic usize, .release writes / .monotonic reads under lock)
//!          Write cursor, advanced by producers under the producer lock.
//!   tail  (atomic usize, .release writes / .acquire reads)
//!          Read cursor, advanced by the consumer only.
//!   slots ([max_events]Slot) — fixed ring buffer, indexed modulo max_events.
//!
//! Producer protocol (under producer_lock):
//!   1. load head (.monotonic)    — safe; only we write it (lock held)
//!   2. load tail (.acquire)      — see consumer's most-recent tail update
//!   3. check head -% tail < max_events; otherwise return QueueFull
//!   4. write slot[head % max_events]
//!   5. store head+1 (.release)   — publishes slot data to consumer
//!
//! Consumer protocol (single thread, no lock):
//!   1. load tail (.monotonic)    — consumer-private; only we write it
//!   2. load head (.acquire)      — see all slot data written by producers
//!   3. iterate tail..head, dispatching each slot to the broker
//!   4. store tail (.release)     — lets producers post into freed slots
//!
//! The full-width usize counters never wrap in practice on 64-bit targets.
//! On 32-bit targets, wrapping arithmetic (`+%`, `-%`) is used throughout
//! so the modulo indexing and fullness check remain correct at rollover.
//!
//! ### Public types and function signatures
//!
//! ```
//! pub const NullLock = struct { ... }  // no-op lock for SPSC use
//!
//! pub fn EventQueue(
//!     comptime max_events: usize,          // ring buffer capacity
//!     comptime max_topic_len: usize,       // max bytes in any topic string
//!     comptime max_data_size: usize,       // max payload bytes (0 = signal-only)
//!     comptime data_alignment: usize,      // alignment of inline data region
//!     comptime Lock: type,                 // producer-side lock
//! ) type
//!
//! // Returned type:
//! pub fn init(producer_lock: Lock) Self
//! pub fn tryPost(
//!     self: *Self,
//!     topic: []const u8,
//!     data: ?[]const u8,
//! ) error{ QueueFull, TopicTooLong, DataTooLarge }!void
//! pub fn drain(self: *Self, broker: anytype) void
//! pub fn isEmpty(self: *const Self) bool
//! pub fn isFull(self: *const Self) bool
//! pub fn pendingCount(self: *const Self) usize
//!
//! pub fn SpscEventQueue(
//!     comptime max_events: usize,
//!     comptime max_topic_len: usize,
//!     comptime max_data_size: usize,
//!     comptime data_alignment: usize,
//! ) type   // convenience alias: EventQueue(..., NullLock)
//! ```
//!
//! ### `drain` and the broker contract
//!
//! `drain` calls `broker.publish(topic_slice, data_ptr_or_null)` once per
//! queued event, in FIFO order.  `broker` is `anytype`; it must expose:
//!
//!   `fn publish(self: *T, topic: []const u8, data: ?*anyopaque) void`
//!
//! This matches `Broker` and `StaticBroker` directly, and matches
//! `LockedBroker` wrapping either of those.  `TypedBroker` is NOT directly
//! compatible — subscribers on a typed broker that receive events from an
//! `EventQueue` should accept `*const Payload` cast from the raw bytes via
//! the untyped `Broker` or `StaticBroker` sitting underneath.
//!
//! When `max_data_size == 0` (signal-only), `data` is always `null`.
//! When `data` passed to `tryPost` is `null`, `data` in the callback is `null`
//! regardless of `max_data_size`.
//! The `data` pointer passed to `broker.publish` points into the slot's inline
//! `data_buf` and is valid only for the duration of that `publish` call — do
//! not retain it across calls.
//!
//! ### Allocator strategy
//!
//! None.  All storage — slots, head, tail, lock — lives inline in the struct.
//! Valid as a global, static local, or heap allocation.  Compiles for
//! freestanding / bare-metal without any OS dependency.
//!
//! ### Comptime constraints and platform gates
//!
//! - `max_events >= 1` — asserted at comptime.
//! - `max_topic_len >= 1` — asserted at comptime.
//! - `max_data_size == 0` is legal (signal-only queue).
//! - `data_alignment` must be a power of two >= 1.
//! - `Lock` duck-typed: must expose `fn lock(*Lock) void` and `fn unlock(*Lock) void`.
//! - Uses `std.atomic.Value(usize)` for head/tail — freestanding-safe; no OS calls.

const std = @import("std");

// ---------------------------------------------------------------------------
// NullLock
// ---------------------------------------------------------------------------

/// A no-op lock for single-producer (SPSC) use.
///
/// All methods are empty and compile away entirely in optimised builds.
/// **Do NOT use when multiple threads call `tryPost` concurrently** — use
/// `SpinLock` (from `sync_broker.zig`) or `std.Thread.Mutex` instead.
pub const NullLock = struct {
    pub fn init() NullLock {
        return .{};
    }
    pub fn lock(self: *NullLock) void {
        _ = self;
    }
    pub fn unlock(self: *NullLock) void {
        _ = self;
    }
};

// ---------------------------------------------------------------------------
// EventQueue
// ---------------------------------------------------------------------------

/// MPSC ring buffer for cross-thread broker dispatch.
///
/// See module doc for full usage and concurrency model.
pub fn EventQueue(
    comptime max_events: usize,
    comptime max_topic_len: usize,
    comptime max_data_size: usize,
    comptime data_alignment: usize,
    comptime Lock: type,
) type {
    comptime {
        if (max_events == 0)
            @compileError("EventQueue: max_events must be >= 1");
        if (max_topic_len == 0)
            @compileError("EventQueue: max_topic_len must be >= 1");
        if (data_alignment == 0 or (data_alignment & (data_alignment - 1)) != 0)
            @compileError("EventQueue: data_alignment must be a power of two >= 1");
        if (!@hasDecl(Lock, "lock"))
            @compileError("EventQueue: Lock must have a `lock` method");
        if (!@hasDecl(Lock, "unlock"))
            @compileError("EventQueue: Lock must have an `unlock` method");
    }

    return struct {
        const Self = @This();

        /// One entry in the ring buffer.
        ///
        /// Topic and payload are stored by value so the producer's memory is
        /// not referenced after `tryPost` returns.  `data_len == 0` means
        /// no payload (signal-only or producer passed `null`).
        const Slot = struct {
            // Effective alignment for `data_buf`: `data_alignment` when storage
            // is present, natural alignment (1) for the zero-length no-op field.
            const buf_align: usize = if (max_data_size > 0) data_alignment else 1;

            topic_buf: [max_topic_len]u8,
            topic_len: usize,
            /// Inline payload storage.  When `max_data_size == 0` this is a
            /// `[0]u8` field that occupies no space in the struct.
            ///
            /// `align(buf_align)` is placed after the complete `if…else` type
            /// expression so the parser does not misread it as the field-level
            /// alignment specifier mid-expression (Zig 0.16 grammar change).
            data_buf: if (max_data_size > 0) [max_data_size]u8 else [0]u8 align(buf_align),
            /// Number of valid bytes in `data_buf`.  0 means no payload.
            data_len: usize,
        };

        /// Ring buffer slots.  Indexed as `slots[head %% max_events]`.
        slots: [max_events]Slot,

        /// Write cursor.  Advanced by producers (under `producer_lock`).
        /// Written with `.release`; consumer reads with `.acquire`.
        head: std.atomic.Value(usize),

        /// Read cursor.  Advanced by the consumer only.
        /// Written with `.release`; producers read with `.acquire` (under lock).
        tail: std.atomic.Value(usize),

        /// Serialises concurrent producers.  The consumer never acquires this.
        producer_lock: Lock,

        /// Initialise an empty queue with the given `producer_lock` instance.
        ///
        /// Example — MPSC with SpinLock:
        ///
        ///   const sync_broker = @import("sync_broker.zig");
        ///   const Q = EventQueue(16, 64, 32, 4, sync_broker.SpinLock);
        ///   var q = Q.init(sync_broker.SpinLock.init());
        ///
        /// Example — SPSC with NullLock via the SpscEventQueue alias:
        ///
        ///   const Q = SpscEventQueue(16, 64, 32, 4);
        ///   var q = Q.init(NullLock.init());
        pub fn init(producer_lock: Lock) Self {
            return .{
                .slots = undefined,
                .head = std.atomic.Value(usize).init(0),
                .tail = std.atomic.Value(usize).init(0),
                .producer_lock = producer_lock,
            };
        }

        /// Post a `(topic, data)` event to the queue.
        ///
        /// Safe to call from an ISR or any producer thread; never blocks.
        /// The contents of `topic` and `data` are **copied** into the slot —
        /// the caller's memory is not referenced after this returns.
        ///
        /// `data` may be `null` to publish a signal with no payload.
        ///
        /// Errors:
        ///   `error.QueueFull`    — all `max_events` slots are occupied.
        ///   `error.TopicTooLong` — `topic.len > max_topic_len`.
        ///   `error.DataTooLarge` — `data.?.len > max_data_size`.
        pub fn tryPost(
            self: *Self,
            topic: []const u8,
            data: ?[]const u8,
        ) error{ QueueFull, TopicTooLong, DataTooLarge }!void {
            // Validate inputs before touching shared state.
            if (topic.len > max_topic_len) return error.TopicTooLong;
            if (data) |d| {
                if (d.len > max_data_size) return error.DataTooLarge;
            }

            self.producer_lock.lock();
            defer self.producer_lock.unlock();

            // .monotonic is safe for head: the lock ensures we are the only writer.
            const head_val = self.head.load(.monotonic);
            // .acquire to see the consumer's most-recent tail.store(.release).
            const tail_val = self.tail.load(.acquire);

            // Wrapping subtraction handles usize rollover on 32-bit targets.
            if (head_val -% tail_val >= max_events) return error.QueueFull;

            const slot = &self.slots[head_val % max_events];

            // Copy topic into slot.
            @memcpy(slot.topic_buf[0..topic.len], topic);
            slot.topic_len = topic.len;

            // Copy payload into slot (comptime-guarded to avoid dead code when
            // max_data_size == 0 and the data_buf field is a [0]u8).
            if (data) |d| {
                if (comptime max_data_size > 0) {
                    @memcpy(slot.data_buf[0..d.len], d);
                }
                slot.data_len = d.len;
            } else {
                slot.data_len = 0;
            }

            // .release: slot writes above must be visible to the consumer
            // before it sees the incremented head.
            self.head.store(head_val +% 1, .release);
        }

        /// Drain all pending events into `broker`, calling `broker.publish`
        /// once per event in FIFO order.
        ///
        /// Must be called from a **single** consumer thread.  Multiple threads
        /// calling `drain` concurrently is undefined behaviour.
        ///
        /// `broker` must expose:
        ///   `fn publish(self: *T, topic: []const u8, data: ?*anyopaque) void`
        ///
        /// When `max_data_size == 0` or `data` was posted as `null`, the
        /// `data` argument to `broker.publish` is `null`.  Otherwise it is a
        /// `?*anyopaque` pointing into the slot's inline `data_buf`; this
        /// pointer is valid only for the duration of the `publish` call — do
        /// not retain it across calls.
        ///
        /// Events posted by producers AFTER `drain` has loaded `head` are NOT
        /// delivered in this call; they will be delivered on the next `drain`.
        pub fn drain(self: *Self, broker: anytype) void {
            // .monotonic: only we (the consumer) write tail.
            var tail_val = self.tail.load(.monotonic);
            // .acquire: see all slot data written by producers before their
            // head.store(.release).
            const head_val = self.head.load(.acquire);

            while (tail_val != head_val) {
                const slot = &self.slots[tail_val % max_events];
                const topic = slot.topic_buf[0..slot.topic_len];

                // Build the data pointer (comptime-gated so that the zero-size
                // `data_buf` field is never addressed when max_data_size == 0).
                const data_ptr: ?*anyopaque = blk: {
                    if (comptime max_data_size > 0) {
                        if (slot.data_len > 0) {
                            break :blk @ptrCast(&slot.data_buf);
                        }
                    }
                    break :blk null;
                };

                broker.publish(topic, data_ptr);
                tail_val +%= 1;
            }

            // .release: let producers see the freed slots via their
            // tail.load(.acquire).
            self.tail.store(tail_val, .release);
        }

        /// Returns `true` when no events are queued.
        ///
        /// A snapshot only — the result may be stale by the time the caller
        /// acts on it.
        pub fn isEmpty(self: *const Self) bool {
            const tail_val = self.tail.load(.acquire);
            const head_val = self.head.load(.acquire);
            return head_val == tail_val;
        }

        /// Returns `true` when all `max_events` slots are occupied.
        ///
        /// A snapshot only — a concurrent producer may have consumed a slot
        /// by the time the caller acts on it.
        pub fn isFull(self: *const Self) bool {
            const tail_val = self.tail.load(.acquire);
            const head_val = self.head.load(.acquire);
            return head_val -% tail_val >= max_events;
        }

        /// Returns the number of events currently queued.
        ///
        /// A snapshot only.
        pub fn pendingCount(self: *const Self) usize {
            const tail_val = self.tail.load(.acquire);
            const head_val = self.head.load(.acquire);
            return head_val -% tail_val;
        }
    };
}

// ---------------------------------------------------------------------------
// SpscEventQueue convenience alias
// ---------------------------------------------------------------------------

/// Convenience constructor for single-producer, single-consumer queues.
///
/// Equivalent to `EventQueue(max_events, max_topic_len, max_data_size,
/// data_alignment, NullLock)`.
///
/// Initialise with `NullLock.init()`:
///
///   const Q = SpscEventQueue(16, 64, 32, 4);
///   var q = Q.init(NullLock.init());
pub fn SpscEventQueue(
    comptime max_events: usize,
    comptime max_topic_len: usize,
    comptime max_data_size: usize,
    comptime data_alignment: usize,
) type {
    return EventQueue(max_events, max_topic_len, max_data_size, data_alignment, NullLock);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "NullLock: lock and unlock are no-ops" {
    var mu = NullLock.init();
    mu.lock();
    mu.unlock();
}

test "EventQueue comptime: valid parameters compile (signal-only SPSC)" {
    const Q = SpscEventQueue(4, 32, 0, 1);
    var q = Q.init(NullLock.init());
    try std.testing.expect(q.isEmpty());
    try std.testing.expect(!q.isFull());
    try std.testing.expectEqual(@as(usize, 0), q.pendingCount());
}

test "EventQueue comptime: non-zero data with alignment, MPSC with NullLock" {
    const Q = EventQueue(8, 64, 16, 4, NullLock);
    var q = Q.init(NullLock.init());
    try std.testing.expect(q.isEmpty());
    try std.testing.expectEqual(@as(usize, 0), q.pendingCount());
}

test "EventQueue: tryPost returns TopicTooLong when topic exceeds max_topic_len" {
    const Q = SpscEventQueue(4, 4, 0, 1);
    var q = Q.init(NullLock.init());
    // "hello" is 5 bytes > max_topic_len=4
    try std.testing.expectError(error.TopicTooLong, q.tryPost("hello", null));
    // Queue must be empty — no slot was consumed.
    try std.testing.expect(q.isEmpty());
}

test "EventQueue: tryPost returns DataTooLarge when payload exceeds max_data_size" {
    const Q = SpscEventQueue(4, 32, 2, 1);
    var q = Q.init(NullLock.init());
    const big: []const u8 = &.{ 0, 1, 2 }; // 3 bytes > max_data_size=2
    try std.testing.expectError(error.DataTooLarge, q.tryPost("t", big));
    try std.testing.expect(q.isEmpty());
}

test "EventQueue: single post then drain fires broker.publish once" {
    const pubsub = @import("pubsub.zig");
    const Q = SpscEventQueue(4, 32, 0, 1);
    const Broker = pubsub.Broker(.{});

    var q = Q.init(NullLock.init());
    var broker = Broker.init(std.testing.allocator);
    defer broker.deinit();

    const Cb = struct {
        fn cb(topic: []const u8, data: ?*anyopaque, ctx: ?*anyopaque) void {
            _ = topic;
            _ = data;
            const n: *u32 = @ptrCast(@alignCast(ctx.?));
            n.* += 1;
        }
    };

    var count: u32 = 0;
    _ = try broker.subscribe("sensors/temp", Cb.cb, &count);

    try q.tryPost("sensors/temp", null);
    try std.testing.expectEqual(@as(usize, 1), q.pendingCount());

    q.drain(&broker);

    try std.testing.expectEqual(@as(u32, 1), count);
    try std.testing.expect(q.isEmpty());
}

test "EventQueue: drain is a no-op on an empty queue" {
    const pubsub = @import("pubsub.zig");
    const Q = SpscEventQueue(4, 16, 0, 1);
    const Broker = pubsub.Broker(.{});

    var q = Q.init(NullLock.init());
    var broker = Broker.init(std.testing.allocator);
    defer broker.deinit();

    var count: u32 = 0;
    const Cb = struct {
        fn cb(topic: []const u8, data: ?*anyopaque, ctx: ?*anyopaque) void {
            _ = topic;
            _ = data;
            const n: *u32 = @ptrCast(@alignCast(ctx.?));
            n.* += 1;
        }
    };
    _ = try broker.subscribe("t", Cb.cb, &count);

    q.drain(&broker); // empty — must not crash or fire
    try std.testing.expectEqual(@as(u32, 0), count);
}

test "EventQueue: QueueFull returned when all slots occupied" {
    const Q = SpscEventQueue(2, 16, 0, 1);
    var q = Q.init(NullLock.init());

    try q.tryPost("a", null);
    try q.tryPost("b", null);
    try std.testing.expect(q.isFull());
    try std.testing.expectEqual(@as(usize, 2), q.pendingCount());
    try std.testing.expectError(error.QueueFull, q.tryPost("c", null));
}

test "EventQueue: drain delivers events in FIFO order" {
    const pubsub = @import("pubsub.zig");
    const Q = SpscEventQueue(4, 16, 0, 1);
    const Broker = pubsub.Broker(.{});

    var q = Q.init(NullLock.init());
    var broker = Broker.init(std.testing.allocator);
    defer broker.deinit();

    // Each callback records the value of a shared counter then increments it.
    var seq: u32 = 0;
    const State = struct { seq: *u32, fired_at: u32 };
    var state_a = State{ .seq = &seq, .fired_at = 999 };
    var state_b = State{ .seq = &seq, .fired_at = 999 };

    const Cb = struct {
        fn cb(topic: []const u8, data: ?*anyopaque, ctx: ?*anyopaque) void {
            _ = topic;
            _ = data;
            const s: *State = @ptrCast(@alignCast(ctx.?));
            s.fired_at = s.seq.*;
            s.seq.* += 1;
        }
    };

    _ = try broker.subscribe("a", Cb.cb, &state_a);
    _ = try broker.subscribe("b", Cb.cb, &state_b);

    // Post "a" first, then "b" — drain must deliver in that order.
    try q.tryPost("a", null);
    try q.tryPost("b", null);
    q.drain(&broker);

    try std.testing.expectEqual(@as(u32, 0), state_a.fired_at); // "a" fired at seq=0
    try std.testing.expectEqual(@as(u32, 1), state_b.fired_at); // "b" fired at seq=1
}

test "EventQueue: reusable after drain" {
    const pubsub = @import("pubsub.zig");
    const Q = SpscEventQueue(2, 16, 0, 1);
    const Broker = pubsub.Broker(.{});

    var q = Q.init(NullLock.init());
    var broker = Broker.init(std.testing.allocator);
    defer broker.deinit();

    var count: u32 = 0;
    const Cb = struct {
        fn cb(topic: []const u8, data: ?*anyopaque, ctx: ?*anyopaque) void {
            _ = topic;
            _ = data;
            const n: *u32 = @ptrCast(@alignCast(ctx.?));
            n.* += 1;
        }
    };
    _ = try broker.subscribe("t", Cb.cb, &count);

    // First round.
    try q.tryPost("t", null);
    q.drain(&broker);
    try std.testing.expectEqual(@as(u32, 1), count);
    try std.testing.expect(q.isEmpty());

    // Second round — slots must be reusable.
    try q.tryPost("t", null);
    q.drain(&broker);
    try std.testing.expectEqual(@as(u32, 2), count);
    try std.testing.expect(q.isEmpty());
}

test "EventQueue: data bytes are copied and forwarded to broker" {
    const pubsub = @import("pubsub.zig");
    // 4-byte data, 4-byte aligned (matches u32 layout).
    const Q = SpscEventQueue(4, 16, 4, 4);
    const Broker = pubsub.Broker(.{});

    var q = Q.init(NullLock.init());
    var broker = Broker.init(std.testing.allocator);
    defer broker.deinit();

    var received: u32 = 0;
    const Cb = struct {
        fn cb(topic: []const u8, data: ?*anyopaque, ctx: ?*anyopaque) void {
            _ = topic;
            const out: *u32 = @ptrCast(@alignCast(ctx.?));
            if (data) |d| {
                const val: *const u32 = @ptrCast(@alignCast(d));
                out.* = val.*;
            }
        }
    };
    _ = try broker.subscribe("v", Cb.cb, &received);

    const payload: u32 = 0xDEAD_BEEF;
    try q.tryPost("v", std.mem.asBytes(&payload));
    q.drain(&broker);

    try std.testing.expectEqual(@as(u32, 0xDEAD_BEEF), received);
}

test "EventQueue: null data is forwarded as null to broker" {
    const pubsub = @import("pubsub.zig");
    const Q = SpscEventQueue(4, 16, 8, 1);
    const Broker = pubsub.Broker(.{});

    var q = Q.init(NullLock.init());
    var broker = Broker.init(std.testing.allocator);
    defer broker.deinit();

    var data_was_null = false;
    const Cb = struct {
        fn cb(topic: []const u8, data: ?*anyopaque, ctx: ?*anyopaque) void {
            _ = topic;
            const flag: *bool = @ptrCast(@alignCast(ctx.?));
            flag.* = (data == null);
        }
    };
    _ = try broker.subscribe("t", Cb.cb, &data_was_null);

    try q.tryPost("t", null); // explicit null
    q.drain(&broker);
    try std.testing.expect(data_was_null);
}

test "EventQueue: multiple posts fill slots and drain clears all" {
    const pubsub = @import("pubsub.zig");
    const Q = SpscEventQueue(4, 8, 0, 1);
    const Broker = pubsub.Broker(.{});

    var q = Q.init(NullLock.init());
    var broker = Broker.init(std.testing.allocator);
    defer broker.deinit();

    var count: u32 = 0;
    const Cb = struct {
        fn cb(topic: []const u8, data: ?*anyopaque, ctx: ?*anyopaque) void {
            _ = topic;
            _ = data;
            const n: *u32 = @ptrCast(@alignCast(ctx.?));
            n.* += 1;
        }
    };
    _ = try broker.subscribe("t", Cb.cb, &count);

    try q.tryPost("t", null);
    try q.tryPost("t", null);
    try q.tryPost("t", null);
    try std.testing.expectEqual(@as(usize, 3), q.pendingCount());

    q.drain(&broker);
    try std.testing.expectEqual(@as(u32, 3), count);
    try std.testing.expect(q.isEmpty());
}

test "EventQueue: hierarchical bubble-up works through drain" {
    const pubsub = @import("pubsub.zig");
    const Q = SpscEventQueue(4, 32, 0, 1);
    const Broker = pubsub.Broker(.{});

    var q = Q.init(NullLock.init());
    var broker = Broker.init(std.testing.allocator);
    defer broker.deinit();

    var root: u32 = 0;
    var child: u32 = 0;
    const Cb = struct {
        fn cb(topic: []const u8, data: ?*anyopaque, ctx: ?*anyopaque) void {
            _ = topic;
            _ = data;
            const n: *u32 = @ptrCast(@alignCast(ctx.?));
            n.* += 1;
        }
    };
    _ = try broker.subscribe("sensors", Cb.cb, &root);
    _ = try broker.subscribe("sensors/temp", Cb.cb, &child);

    try q.tryPost("sensors/temp", null);
    q.drain(&broker); // bubble-up: fires both root and child

    try std.testing.expectEqual(@as(u32, 1), root);
    try std.testing.expectEqual(@as(u32, 1), child);
}

test "EventQueue: StaticBroker works as the drain target" {
    const static_broker = @import("static_broker.zig");
    const Q = SpscEventQueue(4, 16, 0, 1);
    const SB = static_broker.StaticBroker(&.{ "evt/a", "evt/b" }, 4);

    var q = Q.init(NullLock.init());
    var broker = SB.init();

    var count: u32 = 0;
    const Cb = struct {
        fn cb(topic: []const u8, data: ?*anyopaque, ctx: ?*anyopaque) void {
            _ = topic;
            _ = data;
            const n: *u32 = @ptrCast(@alignCast(ctx.?));
            n.* += 1;
        }
    };
    _ = try broker.subscribe("evt/a", Cb.cb, &count);

    try q.tryPost("evt/a", null);
    q.drain(&broker);
    try std.testing.expectEqual(@as(u32, 1), count);
}
