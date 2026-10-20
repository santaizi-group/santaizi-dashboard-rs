---
layout: home
title: Santaizi API
hero:
  name: Santaizi API v2
  text: 同源 OpenAPI 契约
  tagline: Admin、ServerStatus 与客户端共用同一份 OpenAPI 3.0.3。
  actions:
    - theme: brand
      text: 浏览接口
      link: /reference
    - theme: alt
      text: 下载 OpenAPI
      link: /openapi/v2.yaml
features:
  - title: 统一响应
    details: 单项 {data}，列表 {data, meta}，错误 problem+json。
  - title: 会话与 Token
    details: 浏览器 Cookie + CSRF；自动化客户端 Bearer Token。
  - title: 可生成 SDK
    details: Go 接口与 Axios SDK 由规范生成。
---

## 快速开始

写操作需 Cookie 会话或 API Token。浏览器写请求先调 [`GET /api/v2/auth/session`](/guides/authentication) 取 `csrf_token`，再带 `X-CSRF-Token`。详见 [认证与 CSRF](/guides/authentication)。

```bash
curl -H "Authorization: Bearer $SANTAIZI_API_TOKEN" \
  https://monitor.example.com/api/v2/admin/servers
```

规范：<a href="/openapi/v2.yaml"><code>/openapi/v2.yaml</code></a> · <a href="/openapi/v2.json"><code>/openapi/v2.json</code></a>
