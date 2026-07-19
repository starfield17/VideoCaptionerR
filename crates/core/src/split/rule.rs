//! Compatibility adapter for the domain rule-splitting service.

use videocaptionerr_contracts::error::{VcError, VcResult};
use videocaptionerr_contracts::transcript::Transcript;
use videocaptionerr_domain::subtitle::split as domain_split;

pub use domain_split::RuleSplitConfig;

pub fn rule_split(transcript: &Transcript, config: &RuleSplitConfig) -> VcResult<Transcript> {
    domain_split::rule_split(transcript, config).map_err(VcError::from)
}
