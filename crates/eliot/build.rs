#[path = "../../build/git_identity.rs"]
mod git_identity;
#[path = "../../build/windows_resources.rs"]
mod windows_resources;

fn main() {
    git_identity::emit();
    windows_resources::embed("swarm", "Eliot Swarm Controller (service)");
}
