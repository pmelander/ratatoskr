# Contributing

Contributions are welcome. Please read this before opening a pull request.

## Toolchain

Zig **0.16.0** is required. All tasks go through `zig build` — there is no
Make, npm, or other wrapper.

| Task | Command |
|---|---|
| Build | `zig build` |
| Test | `zig build test` |
| Format check | `zig build fmt-check` |
| Format in place | `zig fmt src build.zig` |

All four must pass before a PR is mergeable.

## Conventions

| Category | Style |
|---|---|
| Types | `PascalCase` |
| Functions and fields | `camelCase` |
| Constants and enum tags | `SCREAMING_SNAKE_CASE` |
| File names | `snake_case` |
| One concern per file | new modules go under `src/` |

## Embedded constraints (non-negotiable)

- **No `std.heap.page_allocator`** in library code. Allocators are
  caller-provided.
- **No `std.os` calls** unless gated behind a `comptime` platform check.
- Prefer `comptime` over runtime branching for platform differences.
- Public API must compile for `freestanding` targets.

## Adding a module

1. Create `src/<name>.zig`.
2. Add `pub const <name> = @import("<name>.zig");` (or hoist individual
   symbols) in `src/root.zig`.
3. Write a design note (see below) as a doc comment at the top of the file
   covering: purpose, public types + signatures, allocator strategy, any
   `comptime` constraints.
4. Stub public functions; leave implementation bodies to be filled in.
5. Run `zig build test` and `zig build fmt-check` before committing.

## Design note format

Every new module or breaking API change must include a short note covering:

1. What the module does and why it belongs in this library.
2. Public types and function signatures.
3. Allocator strategy.
4. Any `comptime` constraints or platform gates.

## Pull requests

- Keep PRs focused: one concern per PR.
- Include or update tests for every changed behaviour.
- `zig build test` and `zig build fmt-check` must both pass.
- Summarise the change in the PR description; update `CHANGELOG.md` under
  `[Unreleased]`.
