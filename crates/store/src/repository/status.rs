pub(crate) trait StatusString {
    fn as_str(&self) -> &'static str;
}

impl StatusString for videocaptionerr_domain::JobStatus {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Done => "done",
            Self::DoneDegraded => "done_degraded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }
}

impl StatusString for videocaptionerr_domain::BatchStatus {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Paused => "paused",
            Self::Done => "done",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }
}

impl StatusString for videocaptionerr_domain::WorkUnitStatus {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Done => "done",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }
}
