# WebSocket 协议

OpenAPI operation 上的 `x-websocket` 扩展关联数据通道。

## 公开运行状态

客户端先通过 `GET /api/v2/public/servers` 获取快照，再连接 `/ws/v2/public/runtime`。服务端发送 UTF-8 JSON 文帧；连接断开后应指数退避重连，并重新拉取 REST 快照。

管理面不提供远程命令、终端或文件数据通道。服务探测和 NAT 均通过独立的类型化 gRPC 服务完成。
