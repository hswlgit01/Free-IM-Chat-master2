# TRTC 一对一通话配置

一对一语音/视频使用纯 TRTC SDK，呼叫邀请、接听、拒绝和挂断仍由 OpenIM 信令承载。直播继续使用 LiveKit，二者配置互不影响。

## 服务端环境变量

TRTC 密钥只允许注入 Chat 服务端，不得写入 YAML、客户端代码或日志：

```text
CHATENV_CHAT_RPC_CHAT_RTC_PROVIDER=trtc
CHATENV_CHAT_RPC_CHAT_RTC_TRTC_SDKAPPID=<SDKAppID>
CHATENV_CHAT_RPC_CHAT_RTC_TRTC_SECRETKEY=<SDKSecretKey>
```

可选：

```text
CHATENV_CHAT_RPC_CHAT_RTC_TOKENTTLSECONDS=3600
```

GitHub Actions 部署使用仓库 Secret `TRTC_SDK_APP_ID` 和 `TRTC_SDK_SECRET_KEY`。密钥缺失时，Chat RPC 会在启动阶段失败，避免悄悄回退到错误配置。

## 接口返回

`POST /user/rtc/get_token` 与兼容入口 `POST /third/rtc/get_token` 共用同一套签发逻辑，TRTC 模式返回：

```json
{
  "provider": "trtc",
  "sdkAppId": 12345678,
  "userSig": "由服务端动态签发",
  "userID": "当前已认证的 IM 用户 ID",
  "roomID": "OpenIM 呼叫信令中的房间 ID",
  "expiresAt": 1700000000,
  "token": "与 userSig 相同的兼容字段"
}
```

服务端会忽略空的请求 identity；若传入 identity，则必须等于当前登录账号对应的 IM 用户 ID，不能替其他用户签发凭证。
