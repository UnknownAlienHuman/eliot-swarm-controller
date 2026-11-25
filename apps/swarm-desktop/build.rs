#[path = "../../build/windows_resources.rs"]
mod windows_resources;

fn main() {
    windows_resources::embed("swarm-desktop", "Eliot Swarm Controller");
}
