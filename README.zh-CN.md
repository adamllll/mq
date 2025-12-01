# MQ - 消息队列实现

[English](README.md) | [简体中文](README.zh-CN.md)

一个受 RabbitMQ 启发的轻量级消息队列实现，使用 Java 和 Spring Boot 构建。

## 技术栈

- **Java**: 17
- **框架**: Spring Boot 3.5.8
- **持久化**: MyBatis + SQLite
- **构建工具**: Maven

## 项目结构

```
mq/
├── src/                  # 源代码
├── docs/                 # 文档
│   ├── course.pdf       # 课程资料
│   ├── notes.md         # 学习笔记
│   ├── assets/          # PDF 提取的图片
│   ├── markdown-images/ # Markdown 嵌入的图片
│   └── 板书/            # 课程板书笔记
├── pom.xml              # Maven 配置
└── README.md            # 本文件
```

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

查看详细的学习笔记和实现细节：
- [学习笔记](docs/notes.md) - 全面的学习笔记
- [课程资料](docs/course.pdf) - 原始课程 PDF

## 开发

这是一个学习项目，实现了核心的消息队列概念，包括：
- 消息持久化
- 队列管理
- 发布者/消费者模式
- 消息路由

## 许可证

教育项目 - 可自由使用和修改。

---

**注意**：这是一个为教育目的创建的学习项目。
