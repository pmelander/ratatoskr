# ratatoskr

<p align="center">
  <img src="ratatoskr.png" alt="Ratatoskr on Yggdrasil" width="420">
</p>

A topic-based publish/subscribe message broker for Zig — a systems and
embedded-friendly port of [Subtopic](https://github.com/pmelander/Subtopic).

In Norse mythology, **Ratatoskr** is the squirrel that runs up and down
Yggdrasil carrying messages between the eagle perched in the branches and the
serpent Níðhöggr gnawing at the roots.  This library does the same thing for
your code.

---

## Features

- **Hierarchical bubble-up routing** — publishing to `"app/ui/button"` also
  notifies every ancestor subscriber (`"app/ui"`, `"app"`).
- **LIFO delivery order** — most-recently-subscribed fires first, matching
  Subtopic's original behaviour.
- **Opaque handles** — `subscribe` returns a `Handle`; pass it to
  `unsubscribe` to cancel exactly that subscription.
- **Three broker variants** for different allocation budgets:
  - `Broker` — heap-backed, dynamic topic set.
  - `TypedBroker(Payload)` — type-safe wrapper; no `?*anyopaque` casts.
  - `StaticBroker(topics, max_subs)` — **zero-allocation**, comptime topic
    set; safe for bare-metal / RTOS targets.
- **Freestanding-safe** — no `std.os` calls; compiles for bare-metal targets.
- **Zig 0.16.0** minimum.

---

## Installation

Add ratatoskr as a dependency in your `build.zig.zon`:

```sh
zig fetch --save git+https://github.com/pmelander/ratatoskr
```

Then wire it up in `build.zig`:

```zig
const ratatoskr = b.dependency("ratatoskr", .{
    .target = target,
    .optimize = optimize,
});
your_module.addImport("ratatoskr", ratatoskr.module("ratatoskr"));
```

---

## Quick start

### Raw broker (zero-overhead, dynamic topics)

```zig
const std = @import("std");
const ratatoskr = @import("ratatoskr");

fn onUiEvent(topic: []const u8, data: ?*anyopaque, ctx: ?*anyopaque) void {
    _ = data; _ = ctx;
    std.debug.print("event on topic: {s}\n", .{topic});
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    var broker = ratatoskr.Broker.init(gpa.allocator());
    defer broker.deinit();

    const handle = try broker.subscribe("app/ui", onUiEvent, null);
    defer broker.unsubscribe(handle);

    broker.publish("app/ui", null);            // exact match
    broker.publish("app/ui/button", null);     // child → bubbles up to "app/ui"
    broker.publish("app/ui/modal/close", null);// grandchild → still fires
}
```

### TypedBroker (type-safe, no casting)

```zig
const SensorReading = struct { celsius: f32 };

const SensorBroker = ratatoskr.TypedBroker(SensorReading);

fn onReading(topic: []const u8, data: *const SensorReading, ctx: ?*anyopaque) void {
    _ = topic; _ = ctx;
    std.debug.print("temp: {d:.1}\n", .{data.celsius});
}

var broker = SensorBroker.init(allocator);
defer broker.deinit();

const h = try broker.subscribe("sensors/temp", onReading, null);
var reading = SensorReading{ .celsius = 36.6 };
broker.publish("sensors/temp", &reading);
broker.unsubscribe(h);
```

### StaticBroker (zero-allocation, freestanding)

```zig
// Topic set and capacity are fixed at compile time — no allocator needed.
const Events = ratatoskr.StaticBroker(
    &.{ "sensors/temp", "sensors/pressure", "ui/button" },
    8,  // max subscribers per topic
);

var broker = Events.init();  // no allocator argument

const h = try broker.subscribe("sensors/temp", onTemp, null);
broker.publish("sensors/temp", &reading);
broker.unsubscribe(h);
```

---

## API reference

All public symbols are re-exported from the package root:

```zig
const ratatoskr = @import("ratatoskr");
// ratatoskr.Broker           — default (non-copying) broker type
// ratatoskr.BrokerWith       — parametric broker constructor
// ratatoskr.BrokerOptions    — options struct for BrokerWith
// ratatoskr.TypedBroker      — type-safe broker wrapper
// ratatoskr.StaticBroker     — zero-allocation broker
// ratatoskr.Handle           — opaque subscription token
// ratatoskr.CallbackFn       — raw callback signature
// ratatoskr.TOPIC_SEPARATOR  — '/'
```

---

### `Broker`

The default heap-backed router.  `Broker` is an alias for
`BrokerWith(.{})` (non-copying, default options).

```zig
// Lifecycle
pub fn init(allocator: std.mem.Allocator) Broker
pub fn deinit(self: *Broker) void

// Subscriptions
pub fn subscribe(self: *Broker, topic: []const u8, callback: CallbackFn, ctx: ?*anyopaque) error{OutOfMemory}!Handle
pub fn unsubscribe(self: *Broker, handle: Handle) void
pub fn subscribeOnce(self: *Broker, topic: []const u8, callback: CallbackFn, ctx: ?*anyopaque, once_buf: *Broker.OnceCtx) error{OutOfMemory}!Handle

// Bulk operations
pub fn clearTopic(self: *Broker, topic: []const u8) void
pub fn clearAll(self: *Broker) void

// Inspection
pub fn subscriberCount(self: *Broker, topic: []const u8) usize
pub fn topicIterator(self: *Broker) std.StringHashMap(SubList).KeyIterator

// Publishing
pub fn publish(self: *Broker, topic: []const u8, data: ?*anyopaque) void
pub fn publishExact(self: *Broker, topic: []const u8, data: ?*anyopaque) void
```

| Function | Description |
|---|---|
| `init` | Create a broker backed by `allocator`. |
| `deinit` | Free all resources; invalidates all outstanding handles. |
| `subscribe` | Register `callback`+`ctx` for `topic`. Returns a `Handle`. |
| `unsubscribe` | Cancel a subscription by handle. No-op for stale handles. |
| `subscribeOnce` | One-shot subscription: auto-unsubscribes before the first callback. |
| `clearTopic` | Remove every subscription for `topic` at once. |
| `clearAll` | Remove all subscriptions; retains map capacity for reuse. |
| `subscriberCount` | Number of active subscribers on exactly `topic`. |
| `topicIterator` | Iterate over topic strings that currently have at least one subscriber. Invalidated by any mutating call. |
| `publish` | Deliver `data` to subscribers of `topic` and all ancestor topics (bubble-up). |
| `publishExact` | Deliver `data` to subscribers of `topic` only — no ancestor walk. |

#### `subscribeOnce` and `OnceCtx`

For single-fire subscriptions, declare a caller-owned `OnceCtx` buffer
(typically stack-allocated) and pass it to `subscribeOnce`:

```zig
var once: ratatoskr.Broker.OnceCtx = undefined;
_ = try broker.subscribeOnce("event", myCallback, &state, &once);
// myCallback fires exactly once; the subscription is then automatically cancelled.
```

`once` must remain alive until the callback fires (or until you manually
call `broker.unsubscribe(returned_handle)` to cancel early).

#### `copy_topics` option (`BrokerWith`)

By default, `Broker` borrows the topic string you pass to `subscribe`.
When `copy_topics = true`, the broker duplicates each topic string so
subscriptions remain valid even if the caller frees or reuses the original:

```zig
var broker = ratatoskr.BrokerWith(.{ .copy_topics = true }).init(allocator);
defer broker.deinit();

const heap_topic = try allocator.dupe(u8, "sensor/temp");
const h = try broker.subscribe(heap_topic, cb, null);
allocator.free(heap_topic);          // safe — broker owns its copy
broker.publish("sensor/temp", null); // still works
broker.unsubscribe(h);
```

---

### `TypedBroker(Payload)`

A comptime wrapper around `Broker` that eliminates `?*anyopaque` casts in
callbacks.  Each `subscribe` allocates one `WrapCtx` record on the heap; use
raw `Broker` if you need zero-per-subscription overhead.

```zig
pub fn TypedBroker(comptime Payload: type) type
```

The returned type exposes the same interface as `Broker` with typed signatures:

```zig
// Typed callback — data is *const Payload (or void when Payload == void)
pub const CallbackFn = *const fn (topic: []const u8, data: DataArg, ctx: ?*anyopaque) void

pub fn init(allocator: std.mem.Allocator) Self
pub fn deinit(self: *Self) void
pub fn subscribe(self: *Self, topic: []const u8, callback: CallbackFn, ctx: ?*anyopaque) error{OutOfMemory}!Handle
pub fn unsubscribe(self: *Self, handle: Handle) void
pub fn clearTopic(self: *Self, topic: []const u8) void
pub fn publish(self: *Self, topic: []const u8, data: DataArg) void
pub fn publishExact(self: *Self, topic: []const u8, data: DataArg) void
```

When `Payload == void`, `publish` takes no data argument (`broker.publish(topic, {})`);
callbacks receive `void`.

---

### `StaticBroker(topics, max_subscribers_per_topic)`

A zero-allocation broker whose topic set and per-topic capacity are fixed at
compile time.  All storage is inline in the struct; no allocator is ever called.

```zig
pub fn StaticBroker(
    comptime topics: []const []const u8,
    comptime max_subscribers_per_topic: usize,
) type
```

The returned type exposes:

```zig
pub fn init() Self                         // no allocator
pub fn subscribe(...)  error{TopicNotFound, TooManySubscribers}!Handle
pub fn unsubscribe(handle: Handle) void
pub fn subscribeOnce(...)  error{TopicNotFound, TooManySubscribers}!Handle
pub fn clearTopic(topic: []const u8) void
pub fn clearAll() void
pub fn subscriberCount(topic: []const u8) usize
pub fn publish(topic: []const u8, data: ?*anyopaque) void
pub fn publishExact(topic: []const u8, data: ?*anyopaque) void
pub const OnceCtx: type
```

`Handle`, `CallbackFn`, and `TOPIC_SEPARATOR` are the same types as in `Broker`.

Publishing to a topic not in the comptime `topics` list is silently ignored for
that level, but in-list ancestors encountered during the hierarchical walk still
fire normally.

**Memory layout**: `[topics.len][max_subscribers_per_topic]Subscriber` — fully
inline, zero heap, worst-case memory is statically bounded and visible in your
map file.

---

### `CallbackFn`

```zig
pub const CallbackFn = *const fn (
    topic: []const u8,   // the subscriber's registered topic (ancestor level)
    data:  ?*anyopaque,  // payload passed to publish; may be null
    ctx:   ?*anyopaque,  // context pointer from subscribe; may be null
) void;
```

Use `ctx` to pass a pointer to your own state — the idiomatic replacement for
closures:

```zig
const MyState = struct { count: u32 = 0 };

fn handler(topic: []const u8, data: ?*anyopaque, ctx: ?*anyopaque) void {
    _ = topic; _ = data;
    const s: *MyState = @ptrCast(@alignCast(ctx.?));
    s.count += 1;
}

var state = MyState{};
const h = try broker.subscribe("events", handler, &state);
```

---

### `Handle`

```zig
pub const Handle = struct {
    topic: []const u8,  // same slice passed to subscribe (not copied by default)
    id:    u64,
};
```

For `Broker` with default options, the `topic` slice is **not** duplicated —
keep the original string alive for as long as the handle is in use.
Use `BrokerWith(.{ .copy_topics = true })` if you need the broker to own the
topic string.  `StaticBroker` always stores a pointer to the comptime constant
(valid for the program lifetime).

---

## Topic routing

Topics are `/`-delimited path strings.  `publish` walks **up** the hierarchy,
notifying each ancestor level in turn:

```
publish("sensors/temp/room1", &reading)
  └─ fires subscribers of "sensors/temp/room1"
  └─ fires subscribers of "sensors/temp"
  └─ fires subscribers of "sensors"
```

Use `publishExact` when ancestors must not receive messages intended for a
specific sub-topic:

```
publishExact("sensors/temp/room1", &reading)
  └─ fires subscribers of "sensors/temp/room1" only
```

Sibling topics are completely independent; subscribing to `"sensors/temp"`
does **not** fire for a publish to `"sensors/humidity"`.

Publishing to a topic does **not** trickle *down* to children.

### Delivery order

Within each topic level, subscribers are fired in **reverse registration
order** (most-recently-subscribed first).  This order is stable across
`unsubscribe` calls.

### Re-entrancy

Calling `publish` or `publishExact` from within a callback dispatched by the
same broker is not supported and triggers a `std.debug.assert` failure in debug
and ReleaseSafe builds.  Design callbacks to defer follow-up publishes (e.g.
via a queue) rather than calling back into the broker directly.

Calling `unsubscribe` from within a callback **is** safe and is used
internally by `subscribeOnce`.

### Topic string rules

- Any UTF-8 string is a valid topic; segments are delimited by `/`.
- An empty string `""` is a valid root-only topic with no ancestors.
- Trailing separators create an empty final segment (`"app/"` ≠ `"app"`);
  avoid them unless intentional.

---

## Embedded / freestanding use

ratatoskr has no platform-specific dependencies:

- No `std.os` or POSIX calls.
- No `std.heap.page_allocator` — pass your own allocator (e.g. a
  `FixedBufferAllocator` backed by a static array).
- `StaticBroker` requires **no allocator at all** — use it on bare-metal
  targets where no heap exists.
- Compiles for any target supported by `zig build -Dtarget=<triple>`.

Example with a static buffer:

```zig
var buf: [4096]u8 = undefined;
var fba = std.heap.FixedBufferAllocator.init(&buf);
var broker = ratatoskr.Broker.init(fba.allocator());
```

Example with StaticBroker on a bare-metal target:

```zig
const Events = ratatoskr.StaticBroker(&.{ "isr/adc", "isr/uart", "ui/tick" }, 4);

// Global — zero runtime initialisation cost.
var broker: Events = Events.init();
```

---

## Building and testing

```sh
zig build              # build the static library
zig build test         # run all tests (91 tests)
zig build fmt-check    # verify formatting (CI-safe, no writes)
zig fmt src build.zig  # format in place
```

Cross-compile:

```sh
zig build -Dtarget=aarch64-linux-musl
zig build -Dtarget=thumb-freestanding-eabi -Doptimize=ReleaseSmall
```

---

## License

GPL-3.0 — see [LICENSE](LICENSE).

Inspired by [Subtopic](https://github.com/pmelander/Subtopic) by Patrik Melander,
originally based on Peter Higgins' port of PubSub from Dojo to jQuery.
