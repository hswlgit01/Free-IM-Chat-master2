# Yep-IM-Chat

## 🧩 功能简介

1. 该仓库实现了业务系统，包括两部分：用户系统和后台管理系统。
2. 该系统依赖于 [Yep-im-server 仓库](https://github.com/Map-finance/Yep-IM-Server/tree/dev)，通过调用即时消息系统的 API 实现丰富的业务功能。
3. 用户系统包括一些常规功能，如用户登录、用户注册、用户信息更新等。
4. 后台管理系统包括提供了 API 管理用户、群组和消息等。

### 🚀 启动顺序

1. 成功启动 [Yep-im-server 仓库](https://github.com/Map-finance/Yep-IM-Server/tree/dev)。
2. 本地拥有etcd服务注册发现,以及mongo,redis数据库
3. 编译 chat `mage`。
4. 启动 chat `mage start`。

## 🛫 本地部署

### 📦 克隆

```bash
git clone git@github.com:Map-finance/Yep-IM-Chat.git
cd Yep-IM-Chat
```

### 🛠 初始化

:computer: 第一次编译前，Linux/Mac 平台下执行：
`bootstrap.sh`会帮你自动安装mage构建工具

```
sh bootstrap.sh
```

:computer: Windows 执行：

```
bootstrap.bat
```

### 🏗 编译

```bash
mage
```

### 🚀 启动

```bash
mage start
```

### :floppy_disk: 或后台启动 收集日志

```
nohup mage start >> _output/logs/chat.log 2>&1 &
```

### :mag_right: 检测

```bash
mage check
```

### 🛑 停止

```bash
mage stop
```

## 📞 如果您想启用音视频通话，请配置 LiveKit
更新“config/ Chat -rpc- Chat”。用来配置LiveKit服务器地址：:

```yaml
liveKit:
  url: "ws://127.0.0.1:7880"  # ws://your-server-ip:7880 or wss://your-domain/path
```

## 启动本地开发环境
本地开发环境启动后,无法调用rpc接口,只用于本地环境开发测试使用,且本地开发环境,用户id为固定值

### 本地安装启动mongodb
mongodb端口用户名密码需要与配置文件保持一致

### 手动启动 chat程序

**windows环境**
- powershell
```powershell
$env:IS_LOCAL_TEST="true"
go run .\cmd\api\chat-api\main.go -c .\config\ -i 0 
```

- cmd
```cmd
set IS_LOCAL_TEST="true"
go run .\cmd\api\chat-api\main.go -c .\config\ -i 0 
```

**mac环境**
```zsh
export IS_LOCAL_TEST="true" go run .\cmd\api\chat-api\main.go -c .\config\ -i 0
```
