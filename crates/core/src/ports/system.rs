use videocaptionerr_domain::UlidStr;

pub trait Clock: Send + Sync {
    fn now_ms(&self) -> u64;
}

pub trait IdGenerator: Send + Sync {
    fn next_id(&self) -> UlidStr;
}
