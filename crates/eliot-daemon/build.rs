#[path = "../../build/git_identity.rs"]
mod git_identity;

fn main() {
    git_identity::emit();
}
