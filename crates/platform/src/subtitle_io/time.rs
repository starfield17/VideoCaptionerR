//! Subtitle timestamp parse/format helpers.

use videocaptionerr_contracts::error::{ErrorCode, VcError, VcResult};

/// Format milliseconds as SRT time: `HH:MM:SS,mmm`.
pub fn format_srt_time(ms: u64) -> String {
    let (h, m, s, millis) = split_ms(ms);
    format!("{h:02}:{m:02}:{s:02},{millis:03}")
}

/// Format milliseconds as VTT time: `HH:MM:SS.mmm`.
pub fn format_vtt_time(ms: u64) -> String {
    let (h, m, s, millis) = split_ms(ms);
    format!("{h:02}:{m:02}:{s:02}.{millis:03}")
}

fn split_ms(ms: u64) -> (u64, u64, u64, u64) {
    let millis = ms % 1000;
    let total_s = ms / 1000;
    let s = total_s % 60;
    let total_m = total_s / 60;
    let m = total_m % 60;
    let h = total_m / 60;
    (h, m, s, millis)
}

/// Parse `HH:MM:SS,mmm` or `H:MM:SS,mmm`.
pub fn parse_srt_time(s: &str) -> VcResult<u64> {
    parse_time_inner(s, ',')
}

/// Parse `HH:MM:SS.mmm` (also tolerates comma).
pub fn parse_vtt_time(s: &str) -> VcResult<u64> {
    let s = s.trim();
    // VTT may use MM:SS.mmm without hours.
    if s.matches(':').count() == 1 {
        let sep = if s.contains('.') { '.' } else { ',' };
        let parts: Vec<&str> = s.split(sep).collect();
        if parts.len() != 2 {
            return Err(bad_time(s));
        }
        let hm: Vec<&str> = parts[0].split(':').collect();
        if hm.len() != 2 {
            return Err(bad_time(s));
        }
        let m: u64 = hm[0].trim().parse().map_err(|_| bad_time(s))?;
        let sec: u64 = hm[1].trim().parse().map_err(|_| bad_time(s))?;
        let millis: u64 = parse_millis(parts[1])?;
        return Ok(((m * 60) + sec) * 1000 + millis);
    }
    if s.contains('.') {
        parse_time_inner(s, '.')
    } else {
        parse_time_inner(s, ',')
    }
}

fn parse_time_inner(s: &str, frac_sep: char) -> VcResult<u64> {
    let s = s.trim();
    let parts: Vec<&str> = s.split(frac_sep).collect();
    if parts.len() != 2 {
        return Err(bad_time(s));
    }
    let hms: Vec<&str> = parts[0].split(':').collect();
    if hms.len() != 3 {
        return Err(bad_time(s));
    }
    let h: u64 = hms[0].trim().parse().map_err(|_| bad_time(s))?;
    let m: u64 = hms[1].trim().parse().map_err(|_| bad_time(s))?;
    let sec: u64 = hms[2].trim().parse().map_err(|_| bad_time(s))?;
    if m >= 60 || sec >= 60 {
        return Err(bad_time(s));
    }
    let millis = parse_millis(parts[1])?;
    Ok(((h * 3600) + (m * 60) + sec) * 1000 + millis)
}

fn parse_millis(raw: &str) -> VcResult<u64> {
    let raw = raw.trim();
    if raw.is_empty() || raw.len() > 3 || !raw.chars().all(|c| c.is_ascii_digit()) {
        return Err(VcError::new(
            ErrorCode::InvalidArgument,
            format!("invalid millisecond field: {raw}"),
        ));
    }
    let mut v: u64 = raw.parse().map_err(|_| {
        VcError::new(
            ErrorCode::InvalidArgument,
            format!("invalid millisecond field: {raw}"),
        )
    })?;
    // "5" -> 500, "50" -> 500, "500" -> 500
    for _ in raw.len()..3 {
        v *= 10;
    }
    Ok(v)
}

fn bad_time(s: &str) -> VcError {
    VcError::new(
        ErrorCode::InvalidArgument,
        format!("invalid timestamp: {s}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn srt_round_trip() {
        assert_eq!(format_srt_time(0), "00:00:00,000");
        assert_eq!(format_srt_time(3_661_234), "01:01:01,234");
        assert_eq!(parse_srt_time("01:01:01,234").unwrap(), 3_661_234);
    }

    #[test]
    fn vtt_round_trip() {
        assert_eq!(format_vtt_time(1500), "00:00:01.500");
        assert_eq!(parse_vtt_time("00:00:01.500").unwrap(), 1500);
        assert_eq!(parse_vtt_time("01:02.500").unwrap(), 62_500);
    }
}
