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
- **Allocator-explicit** — every `Broker` is backed by a caller-supplied
  `std.mem.Allocator`.  No page allocator, no global state.
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

```zig
const std = @import("std");
const ratatoskr = @import("ratatoskr");

fn onUiEvent(topic: []const u8, data: ?*anyopaque, ctx: ?*anyopaque) void {
    _ = data;
    _ = ctx;
    std.debug.print("event on topic: {s}\n", .{topic});
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    var broker = ratatoskr.Broker.init(gpa.allocator());
    defer broker.deinit();

    // Subscribe to the "app/ui" topic — will also fire for any deeper publish.
    const handle = try broker.subscribe("app/ui", onUiEvent, null);
    defer broker.unsubscribe(handle);

    // Fires onUiEvent three times, once per publish.
    broker.publish("app/ui", null);           // exact match
    broker.publish("app/ui/button", null);    // child → bubble up to "app/ui"
    broker.publish("app/ui/modal/close", null); // grandchild → still bubbles up
}
```

---

## API reference

All public symbols are available directly from the package root:

```zig
const ratatoskr = @import("ratatoskr");
// ratatoskr.Broker
// ratatoskr.Handle
// ratatoskr.CallbackFn
// ratatoskr.TOPIC_SEPARATOR
```

### `Broker`

The central message router.  Holds all subscription state.  Not thread-safe;
callers own synchronisation.

```zig
pub fn init(allocator: std.mem.Allocator) Broker
pub fn deinit(self: *Broker) void

pub fn subscribe(
    self:     *Broker,
    topic:    []const u8,
    callback: CallbackFn,
    ctx:      ?*anyopaque,
) error{OutOfMemory}!Handle

pub fn unsubscribe(self: *Broker, handle: Handle) void
pub fn clearTopic(self: *Broker, topic: []const u8) void

pub fn publish(
    self:  *Broker,
    topic: []const u8,
    data:  ?*anyopaque,
) void
```

| Function | Description |
|---|---|
| `init` | Create a broker backed by `allocator`. |
| `deinit` | Free all resources; invalidates all outstanding handles. |
| `subscribe` | Register `callback`+`ctx` for `topic`. Returns a `Handle`. |
| `unsubscribe` | Cancel a subscription by handle. No-op for stale handles. |
| `clearTopic` | Remove every subscription for `topic` at once. |
| `publish` | Deliver `data` to subscribers of `topic` and all ancestor topics. |

### `CallbackFn`

```zig
pub const CallbackFn = *const fn (
    topic: []const u8,   // the subscriber's registered topic (ancestor level)
    data:  ?*anyopaque,  // payload passed to publish; may be null
    ctx:   ?*anyopaque,  // context pointer from subscribe; may be null
) void;
```

Use `ctx` to pass a pointer to your own state — it is the idiomatic
replacement for closures:

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

### `Handle`

```zig
pub const Handle = struct {
    topic: []const u8,  // same slice passed to subscribe — not copied
    id:    u64,
};
```

The `topic` slice is **not** duplicated; the memory you pass to `subscribe`
must remain valid for as long as the handle is in use.

### `TOPIC_SEPARATOR`

```zig
pub const TOPIC_SEPARATOR: u8 = '/';
```

---

## Topic routing

Topics are `/`-delimited path strings.  When you `publish` to a topic,
delivery walks **up** the hierarchy, notifying each ancestor level in turn:

```
publish("sensors/temp/room1", &reading)
  └─ fires subscribers of "sensors/temp/room1"
  └─ fires subscribers of "sensors/temp"
  └─ fires subscribers of "sensors"
```

Sibling topics are completely independent; subscribing to `"sensors/temp"`
does **not** fire for a publish to `"sensors/humidity"`.

Publishing to a topic does **not** trickle *down* to children; it only
bubbles *up* to ancestors.

### Delivery order

Within each topic level, subscribers are fired in **reverse registration
order** (most-recently-subscribed first).  This order is stable across
`unsubscribe` calls.

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
  `FixedBufferAllocator` backed by a static array for bare-metal targets).
- Compiles for any target supported by `zig build -Dtarget=<triple>`.

Example with a static buffer:

```zig
var buf: [4096]u8 = undefined;
var fba = std.heap.FixedBufferAllocator.init(&buf);
var broker = ratatoskr.Broker.init(fba.allocator());
```

---

## Building and testing

```sh
zig build              # build the static library
zig build test         # run all tests
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
