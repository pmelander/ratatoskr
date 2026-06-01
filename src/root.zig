//! ratatoskr — topic-based publish/subscribe message broker for Zig.
//!
//! A systems/embedded-friendly port of Subtopic's hierarchical PubSub:
//!   https://github.com/pmelander/Subtopic
//!
//! Public API is re-exported from this file.  Internal sub-modules live
//! under src/ and are imported here explicitly.
//!
//! Quick start (raw, zero-overhead):
//!
//!   const ratatoskr = @import("ratatoskr");
//!
//!   var broker = ratatoskr.Broker.init(allocator);
//!   defer broker.deinit();
//!
//!   const h = try broker.subscribe("app/ui", myCallback, null);
//!   broker.publish("app/ui/button", &payload); // myCallback fires
//!   broker.unsubscribe(h);
//!
//! Quick start (typed, no ?*anyopaque casting):
//!
//!   var broker = ratatoskr.TypedBroker(SensorReading).init(allocator);
//!   defer broker.deinit();
//!
//!   const h = try broker.subscribe("sensors/temp", onReading, &state);
//!   broker.publish("sensors/temp", &reading);
//!   broker.unsubscribe(h);

const std = @import("std");

// Internal sub-modules — not pub so consumers only see the hoisted names below.
const pubsub = @import("pubsub.zig");
const typed_broker = @import("typed_broker.zig");
const static_broker = @import("static_broker.zig");
const sync_broker = @import("sync_broker.zig");
const event_queue = @import("event_queue.zig");

// Hoist the most-used names to the package root for ergonomic imports.
/// Default (non-copying) broker type. Equivalent to `BrokerWith(.{})`.
/// Topic slices passed to `subscribe` must outlive their Handle.
pub const Broker = pubsub.Broker(.{});
/// Options struct for the parametric broker constructor.
pub const BrokerOptions = pubsub.BrokerOptions;
/// Parametric broker constructor. Use when you need `copy_topics` or future options:
///
///   var b = ratatoskr.BrokerWith(.{ .copy_topics = true }).init(allocator);
pub const BrokerWith = pubsub.Broker;
pub const Handle = pubsub.Handle;
pub const CallbackFn = pubsub.CallbackFn;
pub const TOPIC_SEPARATOR = pubsub.TOPIC_SEPARATOR;
pub const TypedBroker = typed_broker.TypedBroker;
/// Zero-allocation, freestanding-safe broker with a comptime-fixed topic set.
///
///   const Events = ratatoskr.StaticBroker(
///       &.{ "sensors/temp", "ui/button" },
///       8,
///   );
///   var broker = Events.init();
pub const StaticBroker = static_broker.StaticBroker;

// ---- Thread safety and cross-thread dispatch --------------------------------

/// Freestanding-safe spinlock (atomic bool, no OS dependency).
/// Use as the `Lock` parameter for `LockedBroker` or `EventQueue` on
/// bare-metal / RTOS targets.  On hosted targets, prefer `std.Thread.Mutex`.
pub const SpinLock = sync_broker.SpinLock;

/// Wraps any broker type with mutual exclusion.
///
///   const SyncBroker = ratatoskr.LockedBroker(ratatoskr.Broker, std.Thread.Mutex);
///   var b = SyncBroker.init(ratatoskr.Broker.init(alloc), .{});
///
///   const SyncStatic = ratatoskr.LockedBroker(Events, ratatoskr.SpinLock);
///   var b = SyncStatic.init(Events.init(), ratatoskr.SpinLock.init());
pub const LockedBroker = sync_broker.LockedBroker;

/// MPSC ring buffer for cross-thread broker dispatch.
/// Producers call `tryPost`; the consumer calls `drain(broker)`.
///
///   const Q = ratatoskr.EventQueue(16, 64, 32, 4, ratatoskr.SpinLock);
///   var q = Q.init(ratatoskr.SpinLock.init());
pub const EventQueue = event_queue.EventQueue;

/// Convenience alias: `EventQueue` with `NullLock` for single-producer use.
///
///   const Q = ratatoskr.SpscEventQueue(16, 64, 0, 1);
///   var q = Q.init(ratatoskr.NullLock.init());
pub const SpscEventQueue = event_queue.SpscEventQueue;

/// No-op lock for single-producer `EventQueue` use.
/// Compiles away entirely in optimised builds.
pub const NullLock = event_queue.NullLock;

test {
    // refAllDecls only sees pub decls of @This(); explicitly reference each
    // sub-module so all their tests are compiled and discovered by the runner.
    std.testing.refAllDecls(@This());
    std.testing.refAllDecls(pubsub);
    std.testing.refAllDecls(typed_broker);
    std.testing.refAllDecls(static_broker);
    std.testing.refAllDecls(sync_broker);
    std.testing.refAllDecls(event_queue);
}
