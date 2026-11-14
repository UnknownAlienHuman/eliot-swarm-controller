//! Adopted local-backend presets through the single OpenCode provider path.
//!
//! The frozen profile carries `backend` as one normalized preset kind
//! (`strata`, `unsloth`, `lmstudio`, `llama.cpp`, `ollama`, `vllm`,
//! `generic`); these are small presets and metadata mappings, not separate
//! Agent executors. Ollama and vLLM reuse the same OpenAI-compatible
//! provider path. The daemon validates the spelling; this module maps each
//! preset to its documented integration boundaries from the owning harness
//! page (endpoint/API dialect, inventory and loaded-state semantics, tool
//! constraints). No I/O, no process spawning, no secret handling here.

/// One adopted backend preset through the shared OpenAI-compatible route.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendKind {
    Strata,
    Unsloth,
    LmStudio,
    LlamaCpp,
    Ollama,
    Vllm,
    Generic,
}

impl BackendKind {
    /// Parse the daemon-normalized `backend` snapshot value. The daemon
    /// already lowercases and canonicalizes aliases (`llama-cpp` and
    /// `lm-studio`/`unsloth-studio` spellings); this keeps the same mapping
    /// so a snapshot written by config validation always parses here.
    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "strata" => Some(Self::Strata),
            "unsloth" | "unsloth-studio" => Some(Self::Unsloth),
            "lmstudio" | "lm-studio" => Some(Self::LmStudio),
            "llama.cpp" | "llama-cpp" | "llama_cpp" => Some(Self::LlamaCpp),
            "ollama" => Some(Self::Ollama),
            "vllm" => Some(Self::Vllm),
            "generic" => Some(Self::Generic),
            _ => None,
        }
    }

    /// Canonical snapshot spelling, matching daemon normalization.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Strata => "strata",
            Self::Unsloth => "unsloth",
            Self::LmStudio => "lmstudio",
            Self::LlamaCpp => "llama.cpp",
            Self::Ollama => "ollama",
            Self::Vllm => "vllm",
            Self::Generic => "generic",
        }
    }

    /// Passive OpenAI-compatible inventory path appended to the frozen
    /// `provider_base_url`. Discovery is always this plain `GET` with the
    /// scoped key reference when one is configured: it never generates
    /// text, and it never sends `?reload=1` (which can unload running
    /// llama.cpp models) or any load/switch selector.
    pub fn inventory_path(self) -> &'static str {
        "/models"
    }

    /// Backend-specific metadata mapping applied to the inventory and
    /// effective readback. Nonsecret hints only; the frozen profile values
    /// stay the authority and any known mismatch refuses before inference.
    pub fn metadata_hint(self) -> &'static str {
        match self {
            // The selected engine HTTP endpoint serves unknown requested
            // names, so the canonical model/aliases must validate against
            // metadata, never a successful response. Health separates
            // reachable service (`status`) from loaded model (`loaded`).
            Self::Strata => {
                "validate canonical model/aliases from metadata; health loaded flag distinguishes service from loaded model"
            }
            // Scoped key reference with client-owned function tools. A
            // process-forced tool policy or auto-switch load policy on an
            // adopted server is owner-controlled and never changed here;
            // keep shared Agents on one explicit model selector.
            Self::Unsloth => {
                "scoped key with client-owned function tools; respect owner tool/load policy"
            }
            // Prefer one explicitly loaded instance shared by all Agents;
            // read that instance's config.context_length, never the
            // model-level maximum. JIT inventory can list unloaded models
            // and auto-evict can unload the previous selection.
            Self::LmStudio => {
                "share one explicitly loaded instance; read instance context_length, not the model maximum"
            }
            // Adopt a fixed-model server first with the selected model's
            // tool-capable template and function-call path. Keep passive
            // reads passive (props with autoload=false); configure the
            // effective per-slot context, not pooled context.
            Self::LlamaCpp => {
                "fixed-model server with tool-capable template; effective per-slot context"
            }
            // Stateless OpenAI-compatible Responses; select an explicit
            // local model through this same provider path.
            Self::Ollama => "explicit local model through the shared provider path",
            // Tool calling depends on model/parser/template; deployment,
            // clustering and hardware tuning stay outside this increment.
            Self::Vllm => "tool calling depends on model/parser/template",
            Self::Generic => "explicit OpenAI-compatible endpoint through the shared provider path",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backend_kinds_parse_snapshot_spellings_and_aliases() {
        for (raw, expected) in [
            ("strata", BackendKind::Strata),
            ("unsloth", BackendKind::Unsloth),
            ("unsloth-studio", BackendKind::Unsloth),
            ("lmstudio", BackendKind::LmStudio),
            ("lm-studio", BackendKind::LmStudio),
            ("llama.cpp", BackendKind::LlamaCpp),
            ("llama-cpp", BackendKind::LlamaCpp),
            ("ollama", BackendKind::Ollama),
            ("vllm", BackendKind::Vllm),
            ("generic", BackendKind::Generic),
        ] {
            assert_eq!(BackendKind::parse(raw), Some(expected), "{raw}");
            assert_eq!(
                BackendKind::parse(expected.as_str()),
                Some(expected),
                "canonical round trip"
            );
        }
        for raw in ["", "openrouter", "openai", "local", "llama", "studio"] {
            assert_eq!(BackendKind::parse(raw), None, "{raw:?}");
        }
    }

    #[test]
    fn inventory_discovery_stays_passive_for_every_backend() {
        // One passive inventory path for all presets: no reload, load or
        // generation selector is ever appended by discovery.
        for kind in [
            BackendKind::Strata,
            BackendKind::Unsloth,
            BackendKind::LmStudio,
            BackendKind::LlamaCpp,
            BackendKind::Ollama,
            BackendKind::Vllm,
            BackendKind::Generic,
        ] {
            assert_eq!(kind.inventory_path(), "/models");
            assert!(!kind.metadata_hint().is_empty());
        }
    }
}
