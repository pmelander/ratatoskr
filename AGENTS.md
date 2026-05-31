# AGENTS.md

Agent orientation for **ratatoskr** — a Zig 0.16.0 systems/embedded library.

## Toolchain requirement

Zig 0.16.0 is the minimum (`minimum_zig_version` in `build.zig.zon`).  
The build system API (`.root_module =`, `b.addModule`, `b.addFmt`) requires 0.14+ and is **not** compatible with 0.13 or earlier.

## Key commands

| Task | Command |
|---|---|
| Build library | `zig build` |
| Run all tests | `zig build test` |
| Format source in-place | `zig fmt src build.zig` |
| Format check (CI-safe, no writes) | `zig build fmt-check` |
| Cross-compile (example) | `zig build -Dtarget=aarch64-linux-musl` |
| Release build | `zig build -Doptimize=ReleaseSafe` |

There is no `make`, `npm`, or other wrapper — all tasks go through `zig build`.

## Running a focused test

Zig's test runner accepts `--test-filter`:

```
zig build test -- --test-filter "name of test"
```

Substring matching: the filter is a substring, not a regex.

## Project layout

```
build.zig         # build script — single source of truth for steps/options
build.zig.zon     # package manifest (version, deps, published paths)
src/
  root.zig        # library root; public API re-exported here
                  # sub-modules imported explicitly from root.zig
```

Add new sub-modules under `src/` and `@import` them in `src/root.zig`.  
`std.testing.refAllDeclsRecursive(@This())` in the root test block automatically discovers tests in imported sub-modules — no per-file test step needed.

## Module vs artifact

`build.zig` exposes **both**:
- `b.addModule("ratatoskr", ...)` — for Zig consumers using `zig fetch` / `build.zig.zon` deps.
- `b.addStaticLibrary(...)` — for C/C++ or FFI consumers that need the `.a`.

When adding a C API, export from a dedicated `src/c_api.zig` and add it to the static lib, not to the module.

## Adding a dependency

```
zig fetch --save <url>#<hash>
```

This writes the entry into `build.zig.zon` automatically.  
Then consume it in `build.zig` with `b.dependency("name", .{...})`.

## Formatting convention

- Run `zig fmt src build.zig` before committing; `zig build fmt-check` enforces this in CI.
- `zig fmt` is canonical — do not configure an alternative formatter.

## Optimize modes

| Mode | Flag | Use |
|---|---|---|
| Debug (default) | _(omit)_ | local dev, all safety checks on |
| ReleaseSafe | `-Doptimize=ReleaseSafe` | production; safety checks retained |
| ReleaseFast | `-Doptimize=ReleaseFast` | benchmarks |
| ReleaseSmall | `-Doptimize=ReleaseSmall` | embedded size-constrained targets |

Prefer `ReleaseSafe` over `ReleaseFast` for this library unless profiling proves otherwise.

## Embedded / cross-compile notes

- Use `b.standardTargetOptions` (already in `build.zig`) — pass target via `-Dtarget=<triple>`.
- Avoid `std.heap.page_allocator` in library code; accept an `std.mem.Allocator` parameter so callers control memory.
- Avoid `std.os` syscalls that are unavailable on freestanding targets unless gated behind a comptime check.

## Reference docs

When in doubt about language or standard library behaviour, consult:

- Language reference: <https://ziglang.org/documentation/master/>
- Standard library: <https://ziglang.org/documentation/master/std/>
