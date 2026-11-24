#[path = "../../build/windows_resources.rs"]
mod windows_resources;

mod build_brand;

fn main() {
    windows_resources::embed("swarm-desktop", "Eliot Swarm Controller");
    let manifest = std::path::PathBuf::from(
        std::env::var_os("CARGO_MANIFEST_DIR").expect("cargo sets CARGO_MANIFEST_DIR"),
    );
    let out = std::path::PathBuf::from(std::env::var_os("OUT_DIR").expect("cargo sets OUT_DIR"));
    build_brand::generate(
        &manifest.join("design").join("tokens.json"),
        &out.join("brand.rs"),
    );
}
