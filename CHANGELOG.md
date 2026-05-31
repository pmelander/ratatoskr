# Changelog

All notable changes to ratatoskr are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

## [0.1.0] — 2026-05-31

### Added
- `Broker` — central message router backed by a caller-supplied allocator.
- `subscribe(topic, callback, ctx) !Handle` — register a typed-erased callback
  with an optional context pointer.
- `unsubscribe(handle)` — cancel a single subscription; no-op for stale handles.
- `clearTopic(topic)` — remove all subscriptions for a topic at once.
- `publish(topic, data)` — deliver a payload to subscribers of `topic` and
  every ancestor topic (hierarchical bubble-up routing).
- LIFO delivery order within each topic level, stable across unsubscribes.
- `TOPIC_SEPARATOR` compile-time constant (`'/'`).
- Full test suite: 24 tests covering correctness, LIFO order, OOM safety at
  every allocation point, memory leak detection, and edge cases.
- Freestanding-safe: no `std.os` calls, allocator always caller-provided.
- Zig 0.16.0 minimum.
