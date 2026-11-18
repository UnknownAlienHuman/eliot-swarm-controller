#[path = "../../build/windows_resources.rs"]
mod windows_resources;

fn main() {
    windows_resources::embed("swarm", "Eliot Swarm Controller (service)");
}
