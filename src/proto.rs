//! `tonic-prost` 生成的线协议代码（权威定义见 `proto/santaizi.proto`）。

// prost 为 oneof 生成的 enum 天然存在大差异变体（oneof 语义上就是一个联合体，
// 装箱会破坏零成本线协议编解码路径），因此放行该 lint。
#![allow(clippy::large_enum_variant)]

include!(concat!(env!("OUT_DIR"), "/proto.rs"));
