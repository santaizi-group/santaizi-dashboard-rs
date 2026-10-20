# 认证与 CSRF

管理端浏览器使用 OAuth Session Cookie。`GET /api/v2/auth/session` 返回当前用户、能力列表及 CSRF Token；所有写操作携带 `X-CSRF-Token`。

自动化客户端使用 `Authorization: Bearer <token>`。

- 过期或已禁用的 Token 会被拒绝（等同未认证）。
- 只读 Token 可访问管理面全部查看类接口，但不能执行写操作（`POST` / `PUT` / `PATCH` / `DELETE`）。
- 操作权 Token 与管理员会话具备同等写能力。

管理员可通过受认证的详情接口查看并复制服务器连接密钥、Collector 注册 Token 和 API Token。列表与日志不返回完整明文。
