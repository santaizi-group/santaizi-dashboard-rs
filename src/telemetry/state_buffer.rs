//! 内存 STATE 样本缓冲：对齐 Go `service/telemetry/state_buffer.go`。
//!
//! STATE 事件不落证据表；进缓冲供每分钟 rollup 聚合（Phase 4 maintenance）。
//! 键为 event_id（16B），重复批次靠 DO NOTHING 语义天然幂等。

use std::collections::HashMap;
use std::sync::Mutex;

use crate::proto;

#[derive(Debug, Clone)]
pub struct StateSample {
    pub event_id: Vec<u8>,
    pub node_uuid: Vec<u8>,
    pub collected_at_nanos: i64,
    pub state: proto::State,
}

/// 未来时间戳容忍上限（24h）：超过即视为不可卷进的漂移样本，丢弃而不是滞留。
const FUTURE_HORIZON_NANOS: i64 = 24 * 60 * 60 * 1_000_000_000;

#[derive(Debug, Default)]
pub struct StateSampleBuffer {
    samples: Mutex<HashMap<Vec<u8>, StateSample>>,
}

impl StateSampleBuffer {
    pub fn new() -> Self {
        Self::default()
    }

    /// 已存在的 event_id 不覆盖：重复帧保持首见样本。
    pub fn add(
        &self,
        event_id: &[u8],
        node_uuid: &[u8],
        collected_at_nanos: i64,
        state: &proto::State,
    ) {
        if event_id.is_empty() || node_uuid.len() != 16 {
            return;
        }
        let mut samples = self.samples.lock().unwrap_or_else(|err| err.into_inner());
        samples
            .entry(event_id.to_vec())
            .or_insert_with(|| StateSample {
                event_id: event_id.to_vec(),
                node_uuid: node_uuid.to_vec(),
                collected_at_nanos,
                state: state.clone(),
            });
    }

    /// 取走并移除 `collected_at < cutoff` 的样本，供 rollup 消费。
    ///
    /// 同时丢弃时间戳远超 `cutoff` 的样本：探针钟漂（或恶意时钟）会让
    /// `collected_at` 落在未来，`< cutoff` 永远不成立，样本就此滞留在缓冲里
    /// 直到墙钟追上——按 event_id 累积，是无界内存增长。这类样本也不可能
    /// 被正确卷进任何一分钟窗口，直接丢弃。
    pub fn take_before(&self, cutoff_nanos: i64) -> Vec<StateSample> {
        let mut samples = self.samples.lock().unwrap_or_else(|err| err.into_inner());
        let stale: Vec<Vec<u8>> = samples
            .iter()
            .filter(|(_, sample)| {
                sample.collected_at_nanos > cutoff_nanos.saturating_add(FUTURE_HORIZON_NANOS)
            })
            .map(|(key, _)| key.clone())
            .collect();
        if !stale.is_empty() {
            tracing::debug!(dropped = stale.len(), "丢弃时间戳超出容忍范围的 STATE 样本");
            for key in stale {
                samples.remove(&key);
            }
        }
        let keys: Vec<Vec<u8>> = samples
            .iter()
            .filter(|(_, sample)| sample.collected_at_nanos < cutoff_nanos)
            .map(|(key, _)| key.clone())
            .collect();
        keys.into_iter()
            .filter_map(|key| samples.remove(&key))
            .collect()
    }

    pub fn len(&self) -> usize {
        self.samples
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod poison_recovery_tests {
    use super::*;

    /// 锁被污染后必须还能用：某个线程在持锁时 panic 过一次，
    /// 之后每次请求都跟着 panic 等于把一次小故障放大成进程级雪崩。
    #[test]
    fn a_poisoned_lock_still_serves() {
        let buffer = StateSampleBuffer::new();
        buffer.add(&[1u8; 16], &[2u8; 16], 10, &proto::State::default());
        // 在持锁期间 panic，把 Mutex 污染掉。
        let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = buffer.samples.lock().unwrap();
            panic!("poison it");
        }));
        assert!(poisoned.is_err(), "应当捕获到 panic");
        assert!(buffer.samples.is_poisoned(), "锁应当已被污染");

        // 污染后 add / take_before / len 必须照常工作。
        buffer.add(&[3u8; 16], &[4u8; 16], 20, &proto::State::default());
        assert_eq!(buffer.len(), 2);
        let taken = buffer.take_before(15);
        assert_eq!(taken.len(), 1);
        assert_eq!(taken[0].event_id, vec![1u8; 16]);
        assert_eq!(buffer.len(), 1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state() -> proto::State {
        proto::State {
            uptime: 42,
            ..Default::default()
        }
    }

    #[test]
    fn add_is_idempotent_by_event_id() {
        let buffer = StateSampleBuffer::new();
        let event_id = [7u8; 16];
        buffer.add(&event_id, &[1u8; 16], 100, &state());
        let mut later = state();
        later.uptime = 99;
        buffer.add(&event_id, &[1u8; 16], 200, &later);
        assert_eq!(buffer.len(), 1);
        let taken = buffer.take_before(150);
        assert_eq!(taken.len(), 1);
        assert_eq!(taken[0].state.uptime, 42);
        assert!(buffer.is_empty());
    }

    #[test]
    fn take_before_only_drains_old_samples() {
        let buffer = StateSampleBuffer::new();
        buffer.add(&[1u8; 16], &[1u8; 16], 100, &state());
        buffer.add(&[2u8; 16], &[1u8; 16], 200, &state());
        // 对齐 Go TakeBefore：只取走 collected_at < cutoff 的样本。
        let first = buffer.take_before(150);
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].event_id, [1u8; 16].to_vec());
        assert_eq!(buffer.len(), 1);
        let second = buffer.take_before(250);
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].event_id, [2u8; 16].to_vec());
        assert!(buffer.is_empty());
    }

    #[test]
    fn add_rejects_invalid_identity() {
        let buffer = StateSampleBuffer::new();
        buffer.add(&[], &[1u8; 16], 1, &state());
        buffer.add(&[1u8; 16], &[1u8; 15], 1, &state());
        assert!(buffer.is_empty());
    }
}

#[cfg(test)]
mod future_timestamp_tests {
    use super::*;

    fn sample(event_id: &[u8], collected_at_nanos: i64) -> StateSample {
        StateSample {
            event_id: event_id.to_vec(),
            node_uuid: vec![7u8; 16],
            collected_at_nanos,
            state: proto::State::default(),
        }
    }

    #[test]
    fn far_future_samples_are_dropped_not_retained() {
        let buffer = StateSampleBuffer::new();
        let cutoff = 1_000_000_000_000i64;
        buffer.add(
            &[1u8; 16],
            &[7u8; 16],
            cutoff + FUTURE_HORIZON_NANOS + 1,
            &proto::State::default(),
        );
        assert_eq!(buffer.len(), 1, "先确认样本已入缓冲");

        let taken = buffer.take_before(cutoff);
        assert!(taken.is_empty(), "漂移样本不得被卷进窗口");
        assert_eq!(
            buffer.len(),
            0,
            "未来时间戳样本必须被丢弃，否则 `< cutoff` 永不成立、无界滞留"
        );
    }

    #[test]
    fn near_future_samples_are_retained_until_their_window() {
        let buffer = StateSampleBuffer::new();
        let cutoff = 1_000_000_000_000i64;
        // 当前分钟内的正常样本（略晚于分钟边界）必须保留。
        let soon = cutoff + 30 * 1_000_000_000;
        buffer.add(&[2u8; 16], &[7u8; 16], soon, &proto::State::default());
        let taken = buffer.take_before(cutoff);
        assert!(taken.is_empty());
        assert_eq!(buffer.len(), 1, "容忍范围内的样本不得被误丢");

        // 墙钟追上后正常消费。
        let taken = buffer.take_before(soon + 1);
        assert_eq!(taken.len(), 1);
        assert_eq!(taken[0].collected_at_nanos, soon);
        assert_eq!(buffer.len(), 0);
    }

    #[test]
    fn sample_helper_builds_expected_shape() {
        let s = sample(&[9u8; 16], 42);
        assert_eq!(s.collected_at_nanos, 42);
        assert_eq!(s.event_id, vec![9u8; 16]);
    }
}
