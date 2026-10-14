# TaskPrompt inline test module path

`store/task_prompt.rs` declares its tests inline and loads the connected Claude fixture with:

```rust
#[path = "../../task_prompt_v5_offline_fixture.rs"]
mod v5_offline_fixture;
```

Rust resolves that nested `#[path]` through the logical module directory
`store/task_prompt/tests/`. Keeping this directory in the repository lets a
fresh checkout reach the existing sibling fixture without copying it or
changing its module nesting.

Remove this directory only after moving the fixture declaration to a normal
file-backed module or replacing it with an `include!` whose path is relative
to `task_prompt.rs`.
