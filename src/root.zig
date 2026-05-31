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

test {
    // refAllDecls only sees pub decls of @This(); explicitly reference each
    // sub-module so all their tests are compiled and discovered by the runner.
    std.testing.refAllDecls(@This());
    std.testing.refAllDecls(pubsub);
    std.testing.refAllDecls(typed_broker);
}
