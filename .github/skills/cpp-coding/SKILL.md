---
name: cpp-coding
description: Write or review Timeplus/Proton C++20 code covering naming conventions, Proton fences, clang-format, IProcessor patterns, and checkpointing. Use when user asks to write or review C++ code, discuss C++ design or style.
---

# C++ Coding

## Language

C++20. Prefer standard library over custom implementations when appropriate.

## Naming conventions

| Element | Style | Example |
|---------|-------|---------|
| Functions | `lowerCamelCase` | `processStreamData()`, `handleWindowClose()` |
| Variables | `lowercase_with_underscores` | `event_count`, `window_start` |
| Classes | `PascalCase` | `StorageStream`, `AggregatedDataVariants` |
| Constants | `PascalCase` or `UPPER_CASE` | Per existing codebase convention |
| Namespaces | `PascalCase` | `DB::Streaming` |

## Proton fences

```cpp
/// proton: starts
if (isStreamingQuery()) { handleStreamingPath(); }
/// proton: ends
```

- [ ] Use ONLY in ClickHouse-inherited code (upstream-synced files)
- [ ] NEVER fence in `src/Storages/Stream/` or `namespace DB::Streaming`
- [ ] NEVER nest fences

## IProcessor pattern (streaming transforms)

All streaming transforms in `src/Processors/Transforms/Streaming/` follow:

```cpp
class MyTransform : public IProcessor {
    Status prepare() override;  // O(1), non-blocking, check readiness
    void work() override;       // Do actual processing
};
```

Stateful processors MUST also implement:
- [ ] `hasState()` → return true
- [ ] `checkpoint(CheckpointContextPtr)` → serialize state
- [ ] `recover(CheckpointContextPtr)` → restore from checkpoint

## Formatting

Only formats changed code blocks (not entire files). Uses `.clang-format` config at repo root.
The configured brace style is Allman-like: opening braces normally go on a new line.

```bash
git clang-format              # format unstaged changes in-place
git clang-format --staged     # format staged changes in-place
git clang-format --diff       # dry-run: show what would change
git clang-format <commit>     # format changes since <commit>
```

## Comments

Use `///`. Explain **why**, not what:
```cpp
/// Use 10 seconds to allow late events
watermark_delay = 10;
```

## Code review checklist

- [ ] Naming follows conventions table above
- [ ] Proton fences correct (only in upstream code, no nesting)
- [ ] `git clang-format --staged` run on changed code blocks; verify with `git clang-format --diff`
- [ ] Comments explain "why" not "what"
- [ ] Stateful processors implement `hasState()`, `checkpoint()`, `recover()`
- [ ] No circular dependencies introduced (respect cluster hierarchy)
- [ ] Minimize changes in `contrib/` — use proton fences for local patches
