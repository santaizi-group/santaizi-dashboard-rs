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
        let mut samples = self.samples.lock().expect("state buffer lock");
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
    pub fn take_before(&self, cutoff_nanos: i64) -> Vec<StateSample> {
        let mut samples = self.samples.lock().expect("state buffer lock");
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
        self.samples.lock().expect("state buffer lock").len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
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
