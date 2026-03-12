# MQ - 消息队列实现

[English](README.md) | [简体中文](README.zh-CN.md)

这是一个受 RabbitMQ 启发的轻量级消息队列项目，基于 Java 17 与 Spring Boot 实现。应用启动后会先初始化 Spring 上下文，再拉起自定义 TCP Broker，监听端口 `9090`。

## 当前能力

- 支持 `DIRECT`、`FANOUT`、`TOPIC` 三种交换机
- 提供 `ConnectionFactory`、`Connection`、`Channel` 客户端 API
- 已实现交换机/队列声明与删除、绑定与解绑、发布、订阅、ACK
- 采用“内存快速读写 + SQLite 元数据 + 消息文件落盘”的存储模型
- 仓库内自带演示入口：`org.adam.mq.demo.DemoConsumer`、`org.adam.mq.demo.DemoProducer`

## 项目结构

```text
src/main/java/org/adam/mq/
  common/        通用参数对象、请求响应模型、序列化工具
  demo/          可直接运行的生产者/消费者示例
  mqclient/      客户端 SDK（ConnectionFactory / Connection / Channel）
  mqserver/      Broker、路由、消费者管理、持久化实现
src/main/resources/
  application.yaml
  mapper/MetaMapper.xml
src/test/java/org/adam/mq/
  路由、存储、虚拟主机、客户端相关测试
docs/
  RESTART_GUIDE.md
  notes_fixed.md
  course.pdf
data/
  运行期 SQLite 元数据和队列消息文件
```

## 运行说明

- Broker TCP 端口：`9090`
- SQLite 元数据文件：`data/meta.db`
- 队列消息文件目录：`data/<queue-name>/`
- 当前仅实现单虚拟主机：`DefaultVHost`

## 快速开始

### 环境要求

- JDK 17 及以上

仓库已经自带 Maven Wrapper，不需要额外安装 Maven。

### 构建

Windows PowerShell：

```powershell
.\mvnw.cmd clean package -DskipTests
```

macOS / Linux：

```bash
./mvnw clean package -DskipTests
```

### 启动 Broker

Windows PowerShell：

```powershell
.\mvnw.cmd spring-boot:run
```

macOS / Linux：

```bash
./mvnw spring-boot:run
```

仓库里仍然保留了 `src/test/java/org/adam/mq` 下的测试代码，但启动 Broker 并不依赖先跑完整测试集。

### 运行示例

1. 先启动 Broker。
2. 运行 `org.adam.mq.demo.DemoConsumer`。
3. 再运行 `org.adam.mq.demo.DemoProducer`。

示例程序默认连接 `127.0.0.1:9090`，并使用 `demo_exchange` 与 `demo_queue`。

## 当前限制

- 暂未实现认证能力
- 暂未实现多虚拟主机
- 当前仓库没有 REST 接口或管理后台

## 文档

- [学习指南](docs/RESTART_GUIDE.md) - 架构梳理与学习路线
- [整理后的笔记](docs/notes_fixed.md) - 汇总后的实现笔记
- [课程资料](docs/course.pdf) - 原始课程 PDF

## 许可说明

这是一个教学/练习性质的项目，可自由用于学习和修改。
