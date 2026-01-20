# MQ - 消息队列实现

[English](README.md) | [简体中文](README.zh-CN.md)

一个受 RabbitMQ 启发的轻量级消息队列实现，使用 Java 和 Spring Boot 构建。

## 技术栈

- **Java**: 17
- **框架**: Spring Boot 3.5.8
- **持久化**: MyBatis + SQLite
- **构建工具**: Maven

## 架构概览

```
┌─────────────────────────────────────────────────────────────┐
│                      客户端 SDK                              │
│              (Connection / Channel)                          │
└─────────────────────┬───────────────────────────────────────┘
                      │ TCP (二进制协议)
┌─────────────────────▼───────────────────────────────────────┐
│                    BrokerServer                              │
│            (请求解析 / 会话管理)                              │
└─────────────────────┬───────────────────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────────────────┐
│                    VirtualHost                               │
│                 (核心业务逻辑)                                │
├──────────────┬──────────────┬───────────────────────────────┤
│    Router    │ ConsumerMgr  │        存储引擎               │
│  (消息路由)   │  (消息推送)   │  ┌─────────┬─────────────┐  │
│              │              │  │  内存层  │   硬盘层     │  │
│              │              │  │  (RAM)   │(SQLite+文件) │  │
└──────────────┴──────────────┴──┴─────────┴─────────────┴───┘
```

## 项目结构

```
mq/
├── src/main/java/org/adam/mq/
│   ├── common/                    # 通用工具
│   │   ├── Request.java          # 网络请求对象
│   │   ├── Response.java         # 网络响应对象
│   │   ├── Consumer.java         # 消费者回调接口
│   │   ├── BinaryTool.java       # 序列化工具
│   │   └── *Arguments.java       # API 参数类
│   │
│   ├── mqserver/                  # MQ 服务端核心
│   │   ├── BrokerServer.java     # TCP 服务器，协议解析
│   │   ├── VirtualHost.java      # 虚拟主机，核心 API
│   │   │
│   │   ├── core/                 # 领域模型
│   │   │   ├── Exchange.java     # 交换机实体
│   │   │   ├── MSGQueue.java     # 队列实体
│   │   │   ├── Binding.java      # 绑定关系
│   │   │   ├── Message.java      # 消息实体
│   │   │   ├── Router.java       # 路由引擎
│   │   │   └── ConsumerManager.java  # 消费者管理
│   │   │
│   │   ├── datacenter/           # 存储层
│   │   │   ├── MemoryDataCenter.java   # 内存存储
│   │   │   ├── DiskDataCenter.java     # 硬盘持久化
│   │   │   ├── DataBaseManager.java    # SQLite 管理
│   │   │   └── MessageFileManager.java # 消息文件存储
│   │   │
│   │   └── mapper/               # MyBatis 映射
│   │       └── MetaMapper.java   # 元数据持久化
│   │
│   └── mqclient/                  # 客户端 SDK
│       ├── Connection.java       # 连接管理
│       └── Channel.java          # 通道操作
│
├── docs/                          # 文档
│   ├── RESTART_GUIDE.md          # 学习指南
│   ├── notes_fixed.md            # 学习笔记
│   └── 板书/                      # 课程图解
│
└── pom.xml                        # Maven 配置
```

## 已实现功能

### 核心功能

| 功能 | 状态 | 描述 |
|------|------|------|
| 交换机管理 | ✅ | 创建/删除交换机（Direct, Fanout, Topic） |
| 队列管理 | ✅ | 创建/删除队列，支持持久化选项 |
| 绑定管理 | ✅ | 绑定/解绑队列到交换机 |
| 消息发布 | ✅ | 发布消息，支持路由键 |
| 消息订阅 | ✅ | 订阅队列，推送模式 |
| 消息确认 | ✅ | 手动/自动 ACK 确认机制 |

### 交换机类型

| 类型 | 路由规则 | 使用场景 |
|------|----------|----------|
| **Direct** | 精确匹配 RoutingKey == BindingKey | 点对点通信 |
| **Fanout** | 广播到所有绑定的队列 | 系统通知 |
| **Topic** | 支持 `*` 和 `#` 通配符的模式匹配 | 灵活订阅 |

### 网络协议

服务端实现了包含 12 种操作类型的二进制协议：

| 编码 | 操作 | 描述 |
|------|------|------|
| 0x1 | createChannel | 创建通道 |
| 0x2 | closeChannel | 关闭通道 |
| 0x3 | exchangeDeclare | 声明交换机 |
| 0x4 | exchangeDelete | 删除交换机 |
| 0x5 | queueDeclare | 声明队列 |
| 0x6 | queueDelete | 删除队列 |
| 0x7 | queueBind | 绑定队列到交换机 |
| 0x8 | queueUnbind | 解除队列绑定 |
| 0x9 | basicPublish | 发布消息 |
| 0xa | basicConsume | 订阅队列 |
| 0xb | basicAck | 确认消息 |
| 0xc | (响应) | 服务器推送给消费者 |

### 存储架构

- **内存层**: 使用 ConcurrentHashMap 实现高性能读写
- **硬盘层**: SQLite 存储元数据 + 二进制文件存储消息体
- **GC 机制**: 基于复制算法的消息文件垃圾回收

## 快速开始

### 前置要求

- JDK 17 或更高版本
- Maven 3.6+

### 构建

```bash
# 构建项目
./mvnw clean package

# 运行测试
./mvnw test
```

### 运行

```bash
# 运行应用程序
./mvnw spring-boot:run
```

## 文档

- [学习指南](docs/RESTART_GUIDE.md) - 包含架构图的完整学习指南
- [学习笔记](docs/notes_fixed.md) - 详细的实现笔记
- [课程资料](docs/course.pdf) - 原始课程 PDF

## 许可证

教育项目 - 可自由使用和修改。

---

**注意**：这是一个为教育目的创建的学习项目，实现了受 RabbitMQ 启发的核心消息队列概念。
