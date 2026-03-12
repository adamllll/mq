# MQ - Message Queue Implementation

[English](README.md) | [简体中文](README.zh-CN.md)

A lightweight RabbitMQ-inspired message queue implemented with Java 17 and Spring Boot. The Spring Boot process initializes the application context and then starts a custom TCP broker on port `9090`.

## Overview

- Exchange types: `DIRECT`, `FANOUT`, `TOPIC`
- Client-side API: `ConnectionFactory`, `Connection`, `Channel`
- Core operations: declare/delete exchange and queue, bind/unbind, publish, consume, ack
- Storage model: in-memory fast path + SQLite metadata + message files on disk
- Demo entry points: `org.adam.mq.demo.DemoConsumer` and `org.adam.mq.demo.DemoProducer`

## Project Layout

```text
src/main/java/org/adam/mq/
  common/        shared DTOs, request/response models, serialization helpers
  demo/          runnable producer and consumer demos
  mqclient/      client SDK (ConnectionFactory / Connection / Channel)
  mqserver/      broker, routing, consumer manager, persistence
src/main/resources/
  application.yaml
  mapper/MetaMapper.xml
src/test/java/org/adam/mq/
  routing, storage, virtual host, and client tests
docs/
  RESTART_GUIDE.md
  notes_fixed.md
  course.pdf
data/
  runtime SQLite metadata and queue message files
```

## Runtime Notes

- Broker TCP port: `9090`
- SQLite metadata file: `data/meta.db`
- Queue message files: `data/<queue-name>/`
- Current implementation uses a single virtual host: `DefaultVHost`

## Quick Start

### Requirement

- JDK 17+

The repository already includes the Maven Wrapper, so a separate Maven installation is not required.

### Build

Windows PowerShell:

```powershell
.\mvnw.cmd clean package -DskipTests
```

macOS / Linux:

```bash
./mvnw clean package -DskipTests
```

### Start The Broker

Windows PowerShell:

```powershell
.\mvnw.cmd spring-boot:run
```

macOS / Linux:

```bash
./mvnw spring-boot:run
```

The repository also contains tests under `src/test/java/org/adam/mq`, but they are not required to start the broker.

### Run The Demo

1. Start the broker first.
2. Run `org.adam.mq.demo.DemoConsumer`.
3. Run `org.adam.mq.demo.DemoProducer`.

The demo connects to `127.0.0.1:9090` and uses `demo_exchange` plus `demo_queue`.

## Limitations

- No authentication support yet
- No multi-vhost support yet
- No REST or admin UI layer in this repository

## Documentation

- [Learning Guide](docs/RESTART_GUIDE.md) - architecture walkthrough and study notes
- [Fixed Notes](docs/notes_fixed.md) - consolidated implementation notes
- [Course Material](docs/course.pdf) - original course PDF

## License

Educational project. Use and modify it freely for learning purposes.
