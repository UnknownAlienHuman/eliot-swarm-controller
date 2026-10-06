//! Conservative Cargo workspace scope from two verified captured trees.
//!
//! Unknown or incomplete metadata widens to the configured whole-workspace
//! invocation. This module never treats an unavailable dependency graph as an
//! empty graph.
use super::source::VerifiedSource;
use crate::{
    error::{Error, Result},
    model,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    path::{Path, PathBuf},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScopeMode {
    Wide,
    Narrowed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScopePlan {
    pub version: u32,
    pub mode: ScopeMode,
    /// Cargo `--package` selectors, stable across capture and attempt IDs.
    pub selected_packages: Vec<String>,
    pub excluded_packages: Vec<String>,
    pub changed_paths: Vec<String>,
    pub required_targets: Vec<String>,
    pub selected_targets: Vec<String>,
    /// Required Cargo targets bound to their exact captured package and source.
    pub target_identities: BTreeMap<String, Vec<CargoTargetIdentity>>,
    pub excluded_required_targets: Vec<String>,
    pub coverage_gaps: Vec<String>,
    pub widening_reasons: Vec<String>,
    /// Stable graph identity. Empty when metadata was unavailable or incomplete.
    pub workspace_graph_sha256: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CargoTargetIdentity {
    pub package_name: String,
    pub package_version: String,
    /// Workspace-relative path; absolute host paths are excluded.
    pub manifest_path: String,
    pub name: String,
    pub kinds: BTreeSet<String>,
    /// Workspace-relative path; absolute host paths are excluded.
    pub src_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Package {
    pub name: String,
    pub version: String,
    pub manifest_path: String,
    pub selector: String,
    pub targets: BTreeSet<String>,
    pub target_identities: Vec<CargoTargetIdentity>,
    pub local_dependencies: BTreeSet<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CargoGraph {
    pub packages: Vec<Package>,
    pub ambiguous_targets: BTreeSet<String>,
    pub workspace_root: String,
    pub fingerprint: String,
    pub complete: bool,
}

impl CargoGraph {
    /// Parse Cargo's `metadata --no-deps` response into a path-independent graph.
    pub(crate) fn parse(value: &Value, source_root: &Path) -> Result<Self> {
        let packages_value = value["packages"]
            .as_array()
            .ok_or_else(|| Error::new("CHECK_METADATA", "Cargo metadata omitted packages"))?;
        let members: BTreeSet<String> = value["workspace_members"]
            .as_array()
            .ok_or_else(|| {
                Error::new("CHECK_METADATA", "Cargo metadata omitted workspace members")
            })?
            .iter()
            .map(|id| {
                id.as_str().map(str::to_string).ok_or_else(|| {
                    Error::new("CHECK_METADATA", "invalid workspace member identity")
                })
            })
            .collect::<Result<_>>()?;
        if packages_value.is_empty() || members.is_empty() {
            return Err(Error::new(
                "CHECK_METADATA",
                "Cargo workspace has no captured members",
            ));
        }
        let mut raw_by_id = BTreeMap::new();
        for raw in packages_value {
            let id = raw["id"]
                .as_str()
                .ok_or_else(|| Error::new("CHECK_METADATA", "Cargo package has no identity"))?;
            if members.contains(id) {
                raw_by_id.insert(id.to_string(), raw);
            }
        }
        if raw_by_id.len() != members.len() {
            return Err(Error::new(
                "CHECK_METADATA",
                "Cargo workspace member metadata is incomplete",
            ));
        }

        let root = fs_canonical(source_root)?;
        let mut member_manifest_paths = BTreeMap::<PathBuf, String>::new();
        let mut member_name_by_manifest = BTreeMap::<PathBuf, String>::new();
        for raw in raw_by_id.values() {
            let manifest_abs = raw["manifest_path"].as_str().ok_or_else(|| {
                Error::new("CHECK_METADATA", "Cargo package has no manifest path")
            })?;
            let manifest = fs_canonical(Path::new(manifest_abs))?;
            let relative = relative_path(&root, &manifest)?;
            let name = raw["name"]
                .as_str()
                .ok_or_else(|| Error::new("CHECK_METADATA", "Cargo package has no name"))?;
            if member_manifest_paths
                .insert(manifest.clone(), relative)
                .is_some()
            {
                return Err(Error::new(
                    "CHECK_METADATA",
                    "Cargo workspace members have an ambiguous manifest path",
                ));
            }
            member_name_by_manifest.insert(manifest, name.to_string());
        }
        let mut packages = Vec::new();
        let mut target_name_counts = BTreeMap::<String, usize>::new();
        let mut complete = true;
        for raw in raw_by_id.values() {
            let name = raw["name"]
                .as_str()
                .ok_or_else(|| Error::new("CHECK_METADATA", "Cargo package has no name"))?
                .to_string();
            let version = raw["version"]
                .as_str()
                .ok_or_else(|| Error::new("CHECK_METADATA", "Cargo package has no version"))?
                .to_string();
            let manifest_abs = raw["manifest_path"].as_str().ok_or_else(|| {
                Error::new("CHECK_METADATA", "Cargo package has no manifest path")
            })?;
            let manifest_path = relative_path(&root, Path::new(manifest_abs))?;
            let mut target_identities = Vec::new();
            for target in raw["targets"]
                .as_array()
                .ok_or_else(|| Error::new("CHECK_METADATA", "Cargo package omitted targets"))?
            {
                let target_name = target["name"]
                    .as_str()
                    .ok_or_else(|| Error::new("CHECK_METADATA", "Cargo target has no name"))?
                    .to_string();
                let kinds: BTreeSet<String> = target["kind"]
                    .as_array()
                    .ok_or_else(|| Error::new("CHECK_METADATA", "Cargo target omitted kinds"))?
                    .iter()
                    .map(|kind| {
                        kind.as_str().map(str::to_string).ok_or_else(|| {
                            Error::new("CHECK_METADATA", "invalid Cargo target kind")
                        })
                    })
                    .collect::<Result<_>>()?;
                if kinds.is_empty() {
                    return Err(Error::new("CHECK_METADATA", "Cargo target has no kinds"));
                }
                let src_path = target["src_path"].as_str().ok_or_else(|| {
                    Error::new("CHECK_METADATA", "Cargo target omitted source path")
                })?;
                *target_name_counts.entry(target_name.clone()).or_default() += 1;
                target_identities.push(CargoTargetIdentity {
                    package_name: name.clone(),
                    package_version: version.clone(),
                    manifest_path: manifest_path.clone(),
                    name: target_name,
                    kinds,
                    src_path: relative_path(&root, Path::new(src_path))?,
                });
            }
            let targets: BTreeSet<String> = target_identities
                .iter()
                .map(|target| target.name.clone())
                .collect();
            if targets.is_empty() {
                complete = false;
            }
            let dependencies = raw["dependencies"].as_array().ok_or_else(|| {
                Error::new("CHECK_METADATA", "Cargo package omitted dependencies")
            })?;
            let mut local_dependencies = BTreeSet::new();
            for dependency in dependencies {
                let dependency_name = dependency["name"].as_str().ok_or_else(|| {
                    Error::new("CHECK_METADATA", "Cargo dependency has no package name")
                })?;
                // Registry dependencies have a source. Cargo includes `path` for a
                // path dependency; resolve that path to the exact captured workspace
                // member rather than guessing from a possibly-colliding package name.
                if dependency["source"].is_null() {
                    let Some(path) = dependency["path"].as_str() else {
                        complete = false;
                        continue;
                    };
                    let dependency_path = Path::new(path);
                    let dependency_path = if dependency_path.is_absolute() {
                        dependency_path.to_path_buf()
                    } else {
                        Path::new(manifest_abs)
                            .parent()
                            .ok_or_else(|| {
                                Error::new(
                                    "CHECK_METADATA",
                                    "Cargo package manifest has no parent directory",
                                )
                            })?
                            .join(dependency_path)
                    };
                    let path = match fs_canonical(&dependency_path) {
                        Ok(path) => path,
                        Err(_) => {
                            complete = false;
                            continue;
                        }
                    };
                    let manifest = if path.is_dir() {
                        path.join("Cargo.toml")
                    } else if path.file_name().is_some_and(|name| name == "Cargo.toml") {
                        path
                    } else {
                        complete = false;
                        continue;
                    };
                    let manifest = match fs_canonical(&manifest) {
                        Ok(path) => path,
                        Err(_) => {
                            complete = false;
                            continue;
                        }
                    };
                    if let Some(member_manifest) = member_manifest_paths.get(&manifest) {
                        if member_name_by_manifest.get(&manifest).map(String::as_str)
                            != Some(dependency_name)
                        {
                            complete = false;
                            continue;
                        }
                        local_dependencies.insert(member_manifest.clone());
                    } else {
                        // This includes an external path dependency with the same
                        // package name as a workspace member. Its contents and any
                        // transitive edge are not proved by this captured graph.
                        complete = false;
                    }
                }
            }
            packages.push(Package {
                name,
                version,
                manifest_path,
                selector: String::new(),
                targets,
                target_identities,
                local_dependencies,
            });
        }
        packages.sort_by(|a, b| a.manifest_path.cmp(&b.manifest_path));
        let ambiguous_targets: BTreeSet<_> = target_name_counts
            .into_iter()
            .filter_map(|(name, count)| (count > 1).then_some(name))
            .collect();
        let mut member_by_name: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for package in &packages {
            member_by_name
                .entry(package.name.clone())
                .or_default()
                .push(package.version.clone());
        }
        for package in &mut packages {
            let versions = member_by_name
                .get(&package.name)
                .cloned()
                .unwrap_or_default();
            let selector = if versions.len() == 1 {
                package.name.clone()
            } else if versions.iter().collect::<BTreeSet<_>>().len() == versions.len() {
                format!("{}@{}", package.name, package.version)
            } else {
                // Cargo's name@version selector cannot disambiguate duplicate
                // workspace packages with identical name and version.
                complete = false;
                format!("{}@{}", package.name, package.version)
            };
            package.selector = selector;
        }
        let workspace_root = value["workspace_root"]
            .as_str()
            .ok_or_else(|| Error::new("CHECK_METADATA", "Cargo metadata omitted workspace root"))?;
        let workspace_root = relative_path(&root, Path::new(workspace_root))?;
        let graph_identity = json!({
            "version": 1,
            "workspace_root": workspace_root,
            "ambiguous_targets": ambiguous_targets,
            "packages": packages.iter().map(|package| json!({
                "name": package.name,
                "version": package.version,
                "manifest_path": package.manifest_path,
                "targets": package.target_identities,
                "local_dependencies": package.local_dependencies,
            })).collect::<Vec<_>>(),
        });
        let fingerprint = model::digest(model::canonical(&graph_identity)?.as_bytes());
        Ok(Self {
            packages,
            ambiguous_targets,
            workspace_root,
            fingerprint,
            complete,
        })
    }
}

fn fs_canonical(path: &Path) -> Result<PathBuf> {
    Ok(std::fs::canonicalize(path)?)
}

fn relative_path(root: &Path, path: &Path) -> Result<String> {
    let absolute = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    let relative = absolute.strip_prefix(root).map_err(|_| {
        Error::new(
            "CHECK_METADATA",
            "Cargo metadata refers outside the captured source",
        )
    })?;
    Ok(relative.to_string_lossy().replace('\\', "/"))
}

fn file_map(source: &VerifiedSource) -> BTreeMap<String, (String, u64, String)> {
    source
        .manifest
        .files
        .iter()
        .map(|file| {
            (
                file.path.clone(),
                (file.sha256.clone(), file.byte_length, file.mode.clone()),
            )
        })
        .collect()
}

fn package_root(manifest_path: &str) -> &str {
    manifest_path
        .rsplit_once('/')
        .map_or("", |(parent, _)| parent)
}

fn under_package(path: &str, root: &str) -> bool {
    if root.is_empty() {
        true
    } else {
        path.strip_prefix(root)
            .is_some_and(|tail| tail.starts_with('/'))
    }
}

fn shared_input(path: &str) -> Option<&'static str> {
    let lower = path.to_ascii_lowercase();
    let basename = lower.rsplit('/').next().unwrap_or(&lower);
    if basename == "cargo.lock" {
        return Some("shared_lockfile");
    }
    if lower == ".cargo/config" || lower == ".cargo/config.toml" || lower.contains("/.cargo/config")
    {
        return Some("shared_cargo_config");
    }
    if matches!(basename, "rust-toolchain" | "rust-toolchain.toml") {
        return Some("toolchain_selection_changed");
    }
    if basename == "build.rs"
        || ["codegen/", "generated/", "schema/", "proto/"]
            .iter()
            .any(|part| lower.starts_with(part) || lower.contains(&format!("/{part}")))
        || [".proto", ".thrift", ".graphql", ".graphqls", ".wit"]
            .iter()
            .any(|extension| lower.ends_with(extension))
    {
        return Some("shared_codegen_input");
    }
    None
}

fn target_identities_for(
    graph: Option<&CargoGraph>,
    required_targets: &[String],
) -> BTreeMap<String, Vec<CargoTargetIdentity>> {
    let Some(graph) = graph else {
        return BTreeMap::new();
    };
    required_targets
        .iter()
        .map(|name| {
            let mut identities: Vec<_> = graph
                .packages
                .iter()
                .flat_map(|package| package.target_identities.iter())
                .filter(|target| target.name == *name)
                .cloned()
                .collect();
            identities.sort_by(|left, right| {
                left.manifest_path
                    .cmp(&right.manifest_path)
                    .then_with(|| left.kinds.cmp(&right.kinds))
                    .then_with(|| left.src_path.cmp(&right.src_path))
            });
            (name.clone(), identities)
        })
        .collect()
}

/// Compare verified candidate/baseline trees and conservatively close over local
/// workspace dependents. Missing evidence always produces a wide plan.
pub(crate) fn analyze(
    candidate: &VerifiedSource,
    baseline: Option<&VerifiedSource>,
    candidate_graph: Option<&CargoGraph>,
    baseline_graph: Option<&CargoGraph>,
    required_targets: &[String],
) -> ScopePlan {
    let candidate_files = file_map(candidate);
    let baseline_files = baseline.map(file_map);
    let mut changed_paths = BTreeSet::new();
    match &baseline_files {
        Some(base) => {
            for path in candidate_files.keys().chain(base.keys()) {
                if candidate_files.get(path) != base.get(path) {
                    changed_paths.insert(path.clone());
                }
            }
        }
        None => changed_paths.extend(candidate_files.keys().cloned()),
    }

    let mut reasons = BTreeSet::new();
    if baseline.is_none() {
        reasons.insert("baseline_unavailable".to_string());
    }
    let Some(candidate_graph) = candidate_graph else {
        reasons.insert("cargo_metadata_unavailable".to_string());
        return plan_all(None, required_targets, changed_paths, reasons, None);
    };
    if !candidate_graph.complete {
        reasons.insert("dependency_graph_incomplete".to_string());
    }
    if baseline.is_some() {
        let Some(baseline_graph) = baseline_graph else {
            reasons.insert("baseline_metadata_unavailable".to_string());
            return plan_all(
                Some(candidate_graph),
                required_targets,
                changed_paths,
                reasons,
                Some(candidate_graph.fingerprint.clone()),
            );
        };
        if !baseline_graph.complete {
            reasons.insert("baseline_dependency_graph_incomplete".to_string());
        }
        if candidate_graph.fingerprint != baseline_graph.fingerprint {
            reasons.insert("workspace_graph_changed".to_string());
        }
    }
    if changed_paths.is_empty() {
        reasons.insert("no_source_change".to_string());
    }
    for path in &changed_paths {
        if let Some(reason) = shared_input(path) {
            reasons.insert(format!("{reason}:{path}"));
        }
        if !candidate_graph
            .packages
            .iter()
            .any(|package| under_package(path, package_root(&package.manifest_path)))
        {
            reasons.insert(format!("outside_package:{path}"));
        }
    }
    for target in &candidate_graph.ambiguous_targets {
        if required_targets.iter().any(|required| required == target) {
            reasons.insert(format!("required_target_name_ambiguous:{target}"));
        }
    }

    let mut selected = BTreeSet::new();
    if reasons.is_empty() {
        for package in &candidate_graph.packages {
            let root = package_root(&package.manifest_path);
            if changed_paths.iter().any(|path| {
                if root.is_empty() {
                    true
                } else {
                    under_package(path, root)
                }
            }) {
                selected.insert(package.manifest_path.clone());
            }
        }
        let mut queue: VecDeque<String> = selected.iter().cloned().collect();
        while let Some(changed_name) = queue.pop_front() {
            for package in &candidate_graph.packages {
                if !selected.contains(&package.manifest_path)
                    && package.local_dependencies.contains(&changed_name)
                {
                    selected.insert(package.manifest_path.clone());
                    queue.push_back(package.manifest_path.clone());
                }
            }
        }
        if selected.is_empty() {
            reasons.insert("changed_files_unmapped".to_string());
        } else if selected.len() == candidate_graph.packages.len() {
            reasons.insert("reverse_closure_is_workspace".to_string());
        }
    }
    if !reasons.is_empty() {
        return plan_all(
            Some(candidate_graph),
            required_targets,
            changed_paths,
            reasons,
            Some(candidate_graph.fingerprint.clone()),
        );
    }

    let selected_packages: Vec<_> = candidate_graph
        .packages
        .iter()
        .filter(|package| selected.contains(&package.manifest_path))
        .map(|package| package.selector.clone())
        .collect();
    let excluded_packages: Vec<_> = candidate_graph
        .packages
        .iter()
        .filter(|package| !selected.contains(&package.manifest_path))
        .map(|package| package.selector.clone())
        .collect();
    let selected_target_names: BTreeSet<_> = candidate_graph
        .packages
        .iter()
        .filter(|package| selected.contains(&package.manifest_path))
        .flat_map(|package| package.targets.iter().cloned())
        .collect();
    let all_target_names: BTreeSet<_> = candidate_graph
        .packages
        .iter()
        .flat_map(|package| package.targets.iter().cloned())
        .collect();
    let selected_targets: Vec<_> = required_targets
        .iter()
        .filter(|target| selected_target_names.contains(*target))
        .cloned()
        .collect();
    let excluded_required_targets: Vec<_> = required_targets
        .iter()
        .filter(|target| !selected_target_names.contains(*target))
        .cloned()
        .collect();
    let coverage_gaps = excluded_required_targets
        .iter()
        .map(|target| {
            if all_target_names.contains(target) {
                format!("required_target_excluded_by_scope:{target}")
            } else {
                format!("required_target_unknown_to_metadata:{target}")
            }
        })
        .collect();
    ScopePlan {
        version: 1,
        mode: ScopeMode::Narrowed,
        selected_packages,
        excluded_packages,
        changed_paths: changed_paths.into_iter().collect(),
        required_targets: required_targets.to_vec(),
        selected_targets,
        target_identities: target_identities_for(Some(candidate_graph), required_targets),
        excluded_required_targets,
        coverage_gaps,
        widening_reasons: Vec::new(),
        workspace_graph_sha256: Some(candidate_graph.fingerprint.clone()),
    }
}

fn plan_all(
    graph: Option<&CargoGraph>,
    required_targets: &[String],
    changed_paths: BTreeSet<String>,
    reasons: BTreeSet<String>,
    graph_sha256: Option<String>,
) -> ScopePlan {
    let selected_packages = graph
        .map(|graph| {
            graph
                .packages
                .iter()
                .map(|package| package.selector.clone())
                .collect()
        })
        .unwrap_or_default();
    let selected_targets: BTreeSet<String> = graph
        .map(|graph| {
            graph
                .packages
                .iter()
                .flat_map(|package| package.targets.iter().cloned())
                .collect()
        })
        .unwrap_or_default();
    let missing: Vec<_> = if graph.is_some() {
        required_targets
            .iter()
            .filter(|target| !selected_targets.contains(*target))
            .cloned()
            .collect()
    } else {
        Vec::new()
    };
    let ambiguous_targets = graph
        .map(|graph| &graph.ambiguous_targets)
        .cloned()
        .unwrap_or_default();
    let coverage_gaps = required_targets
        .iter()
        .filter_map(|target| {
            if ambiguous_targets.contains(target) {
                Some(format!("required_target_name_ambiguous:{target}"))
            } else if missing.contains(target) {
                Some(format!("required_target_unknown_to_metadata:{target}"))
            } else {
                None
            }
        })
        .collect();
    ScopePlan {
        version: 1,
        mode: ScopeMode::Wide,
        selected_packages,
        excluded_packages: Vec::new(),
        changed_paths: changed_paths.into_iter().collect(),
        required_targets: required_targets.to_vec(),
        selected_targets: if graph.is_some() {
            required_targets
                .iter()
                .filter(|target| selected_targets.contains(*target))
                .cloned()
                .collect()
        } else {
            required_targets.to_vec()
        },
        target_identities: target_identities_for(graph, required_targets),
        excluded_required_targets: missing,
        coverage_gaps,
        widening_reasons: reasons.into_iter().collect(),
        workspace_graph_sha256: graph_sha256,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checks::source::{SourceFile, SourceManifest};

    fn source(directory: &str, files: &[(&str, &str)]) -> VerifiedSource {
        VerifiedSource {
            manifest: SourceManifest {
                version: 1,
                commit: "commit".into(),
                tree: "tree".into(),
                files: files
                    .iter()
                    .map(|(path, hash)| SourceFile {
                        path: (*path).into(),
                        mode: "100644".into(),
                        object_id: "oid".into(),
                        byte_length: 1,
                        sha256: (*hash).into(),
                    })
                    .collect(),
            },
            content_sha256: "content".into(),
            directory: PathBuf::from(directory),
        }
    }

    fn graph() -> CargoGraph {
        let target = |package: &str, name: &str, kind: &str, src_path: &str| CargoTargetIdentity {
            package_name: package.into(),
            package_version: "1.0.0".into(),
            manifest_path: format!("{package}/Cargo.toml"),
            name: name.into(),
            kinds: BTreeSet::from([kind.into()]),
            src_path: src_path.into(),
        };
        CargoGraph {
            packages: vec![
                Package {
                    name: "core".into(),
                    version: "1.0.0".into(),
                    manifest_path: "core/Cargo.toml".into(),
                    selector: "core".into(),
                    targets: BTreeSet::from(["core".into()]),
                    target_identities: vec![target("core", "core", "lib", "core/src/lib.rs")],
                    local_dependencies: BTreeSet::new(),
                },
                Package {
                    name: "app".into(),
                    version: "1.0.0".into(),
                    manifest_path: "app/Cargo.toml".into(),
                    selector: "app".into(),
                    targets: BTreeSet::from(["app".into()]),
                    target_identities: vec![target("app", "app", "bin", "app/src/main.rs")],
                    local_dependencies: BTreeSet::from(["core/Cargo.toml".into()]),
                },
                Package {
                    name: "other".into(),
                    version: "1.0.0".into(),
                    manifest_path: "other/Cargo.toml".into(),
                    selector: "other".into(),
                    targets: BTreeSet::from(["other".into()]),
                    target_identities: vec![target("other", "other", "lib", "other/src/lib.rs")],
                    local_dependencies: BTreeSet::new(),
                },
            ],
            ambiguous_targets: BTreeSet::new(),
            workspace_root: ".".into(),
            fingerprint: "graph".into(),
            complete: true,
        }
    }

    #[test]
    fn cargo_metadata_parser_builds_only_captured_workspace_edges() {
        let root = std::env::temp_dir().join(format!("swarm-graph-{}", model::new_id()));
        std::fs::create_dir_all(&root).unwrap();
        let root = std::fs::canonicalize(root).unwrap();
        for member in ["core", "app"] {
            std::fs::create_dir_all(root.join(member).join("src")).unwrap();
            std::fs::write(root.join(member).join("Cargo.toml"), "[package]\n").unwrap();
        }
        std::fs::write(root.join("core/src/lib.rs"), "pub fn core() {}\n").unwrap();
        std::fs::write(root.join("app/src/main.rs"), "fn main() {}\n").unwrap();
        let metadata = json!({
            "workspace_root": root,
            "workspace_members": ["core-id", "app-id"],
            "packages": [
                {"id":"core-id","name":"core","version":"1.0.0","manifest_path":root.join("core/Cargo.toml"),
                    "targets":[{"name":"shared","kind":["lib"],"src_path":root.join("core/src/lib.rs")}],"dependencies":[]},
                {"id":"app-id","name":"app","version":"1.0.0","manifest_path":root.join("app/Cargo.toml"),
                    "targets":[{"name":"shared","kind":["bin"],"src_path":root.join("app/src/main.rs")}],"dependencies":[{"name":"core","source":null,"path":root.join("core")} ]}
            ]
        });
        let graph = CargoGraph::parse(&metadata, &root).unwrap();
        assert!(graph.complete);
        assert_eq!(graph.ambiguous_targets, BTreeSet::from(["shared".into()]));
        assert_eq!(graph.packages[0].manifest_path, "app/Cargo.toml");
        assert_eq!(graph.packages[1].manifest_path, "core/Cargo.toml");
        assert!(graph.packages[1].local_dependencies.is_empty());
        assert_eq!(
            graph.packages[0].local_dependencies,
            BTreeSet::from(["core/Cargo.toml".into()])
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn external_same_named_path_dependency_makes_graph_incomplete() {
        let root = std::env::temp_dir().join(format!("swarm-graph-external-{}", model::new_id()));
        std::fs::create_dir_all(&root).unwrap();
        let root = std::fs::canonicalize(root).unwrap();
        for directory in ["core", "app", "external/core"] {
            std::fs::create_dir_all(root.join(directory)).unwrap();
            std::fs::write(root.join(directory).join("Cargo.toml"), "[package]\n").unwrap();
        }
        std::fs::create_dir_all(root.join("core/src")).unwrap();
        std::fs::create_dir_all(root.join("app/src")).unwrap();
        std::fs::write(root.join("core/src/lib.rs"), "pub fn core() {}\n").unwrap();
        std::fs::write(root.join("app/src/main.rs"), "fn main() {}\n").unwrap();
        let metadata = json!({
            "workspace_root":root,
            "workspace_members":["core-id","app-id"],
            "packages":[
                {"id":"core-id","name":"core","version":"1.0.0","manifest_path":root.join("core/Cargo.toml"),
                    "targets":[{"name":"core","kind":["lib"],"src_path":root.join("core/src/lib.rs")}],"dependencies":[]},
                {"id":"app-id","name":"app","version":"1.0.0","manifest_path":root.join("app/Cargo.toml"),
                    "targets":[{"name":"app","kind":["bin"],"src_path":root.join("app/src/main.rs")}],"dependencies":[{"name":"core","source":null,"path":root.join("external/core")}]}
            ]
        });
        let graph = CargoGraph::parse(&metadata, &root).unwrap();
        assert!(!graph.complete);
        assert!(graph.packages[0].local_dependencies.is_empty());
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn reverse_closure_selects_dependents_and_reports_excluded_required_targets() {
        let baseline = source(
            "baseline",
            &[
                ("core/src/lib.rs", "old"),
                ("app/src/main.rs", "same"),
                ("other/src/lib.rs", "same"),
            ],
        );
        let candidate = source(
            "candidate",
            &[
                ("core/src/lib.rs", "new"),
                ("app/src/main.rs", "same"),
                ("other/src/lib.rs", "same"),
            ],
        );
        let graph = graph();
        let plan = analyze(
            &candidate,
            Some(&baseline),
            Some(&graph),
            Some(&graph),
            &["core".into(), "app".into(), "other".into()],
        );
        assert_eq!(plan.mode, ScopeMode::Narrowed);
        assert_eq!(plan.selected_packages, ["core", "app"]);
        assert_eq!(plan.excluded_packages, ["other"]);
        assert_eq!(plan.excluded_required_targets, ["other"]);
        assert_eq!(
            plan.coverage_gaps,
            ["required_target_excluded_by_scope:other"]
        );
    }

    #[test]
    fn ambiguous_required_target_name_widens_and_blocks_pass() {
        let baseline = source(
            "baseline",
            &[
                ("core/src/lib.rs", "old"),
                ("app/src/main.rs", "same"),
                ("other/src/lib.rs", "same"),
            ],
        );
        let candidate = source(
            "candidate",
            &[
                ("core/src/lib.rs", "new"),
                ("app/src/main.rs", "same"),
                ("other/src/lib.rs", "same"),
            ],
        );
        let mut graph = graph();
        graph.packages[2].targets.insert("core".into());
        graph.ambiguous_targets.insert("core".into());
        let plan = analyze(
            &candidate,
            Some(&baseline),
            Some(&graph),
            Some(&graph),
            &["core".into()],
        );
        assert_eq!(plan.mode, ScopeMode::Wide);
        assert!(
            plan.widening_reasons
                .contains(&"required_target_name_ambiguous:core".into())
        );
        assert!(
            plan.coverage_gaps
                .contains(&"required_target_name_ambiguous:core".into())
        );
    }

    #[test]
    fn missing_baseline_and_shared_lockfile_widen_to_all_packages() {
        let candidate = source(
            "candidate",
            &[("core/src/lib.rs", "new"), ("Cargo.lock", "lock")],
        );
        let graph = graph();
        let plan = analyze(&candidate, None, Some(&graph), None, &["core".into()]);
        assert_eq!(plan.mode, ScopeMode::Wide);
        assert_eq!(plan.selected_packages, ["core", "app", "other"]);
        assert!(
            plan.widening_reasons
                .contains(&"baseline_unavailable".into())
        );
        assert!(
            plan.widening_reasons
                .iter()
                .any(|reason| reason.starts_with("shared_lockfile:"))
        );
    }

    #[test]
    fn unknown_graph_never_becomes_an_empty_narrow_scope() {
        let baseline = source("baseline", &[("core/src/lib.rs", "old")]);
        let candidate = source("candidate", &[("core/src/lib.rs", "new")]);
        let plan = analyze(&candidate, Some(&baseline), None, None, &["core".into()]);
        assert_eq!(plan.mode, ScopeMode::Wide);
        assert!(plan.selected_packages.is_empty());
        assert!(
            plan.widening_reasons
                .contains(&"cargo_metadata_unavailable".into())
        );
    }

    #[test]
    fn root_codegen_inputs_are_shared_scope_changes() {
        assert_eq!(
            shared_input("codegen/generate.rs"),
            Some("shared_codegen_input")
        );
        assert_eq!(
            shared_input("generated/api.rs"),
            Some("shared_codegen_input")
        );
        assert_eq!(
            shared_input("proto/service.proto"),
            Some("shared_codegen_input")
        );
    }
}
