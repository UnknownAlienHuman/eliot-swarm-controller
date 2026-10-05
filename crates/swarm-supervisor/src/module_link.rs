//! Descriptor-to-hello-claim projection used by the supervisor launch plan.
//!
//! The claim's canonical type lives in `swarm-contracts`; transport and hello
//! framing live in `swarm-client`. This crate only pins the exact descriptor
//! and negotiated protocol into the private per-scope plan.

use crate::{ModuleDescriptor, ProtocolRange};
use swarm_contracts::{
    error::{Error, Result},
    module_contract::ModuleContractClaim,
};

pub fn module_contract_claim(
    descriptor: &ModuleDescriptor,
    host_protocol: ProtocolRange,
) -> Result<ModuleContractClaim> {
    let protocol = negotiate_protocol(descriptor.protocol, host_protocol)?;
    ModuleContractClaim::from_descriptor(descriptor, protocol)
        .map_err(|error| Error::new("MODULE_CONTRACT_INVALID", error.to_string()))
}

fn negotiate_protocol(
    module: ProtocolRange,
    host: ProtocolRange,
) -> Result<swarm_contracts::module_catalog::ProtocolVersion> {
    if module.minimum.major != module.maximum.major
        || host.minimum.major != host.maximum.major
        || module.minimum.minor > module.maximum.minor
        || host.minimum.minor > host.maximum.minor
        || module.minimum.major != host.minimum.major
    {
        return Err(Error::new(
            "MODULE_PROTOCOL_INCOMPATIBLE",
            "module and host protocol ranges do not intersect",
        ));
    }
    let minimum = module.minimum.minor.max(host.minimum.minor);
    let maximum = module.maximum.minor.min(host.maximum.minor);
    if minimum > maximum {
        return Err(Error::new(
            "MODULE_PROTOCOL_INCOMPATIBLE",
            "module and host protocol ranges do not intersect",
        ));
    }
    Ok(swarm_contracts::module_catalog::ProtocolVersion {
        major: module.minimum.major,
        minor: maximum,
    })
}
