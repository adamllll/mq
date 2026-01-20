# MQ - Message Queue Implementation

[English](README.md) | [简体中文](README.zh-CN.md)

A lightweight message queue implementation inspired by RabbitMQ, built with Java and Spring Boot.

## Tech Stack

- **Java**: 17
- **Framework**: Spring Boot 3.5.8
- **Persistence**: MyBatis + SQLite
- **Build Tool**: Maven

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                      Client SDK                              │
│              (Connection / Channel)                          │
└─────────────────────┬───────────────────────────────────────┘
                      │ TCP (Binary Protocol)
┌─────────────────────▼───────────────────────────────────────┐
│                    BrokerServer                              │
│         (Request Parsing / Session Management)               │
└─────────────────────┬───────────────────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────────────────┐
│                    VirtualHost                               │
│              (Core Business Logic)                           │
├──────────────┬──────────────┬───────────────────────────────┤
│    Router    │ ConsumerMgr  │      Storage Engine           │
│  (Routing)   │  (Push Msg)  │  ┌─────────┬─────────────┐   │
│              │              │  │ Memory  │    Disk     │   │
│              │              │  │ (RAM)   │(SQLite+File)│   │
└──────────────┴──────────────┴──┴─────────┴─────────────┴───┘
```

## Project Structure

```
mq/
├── src/main/java/org/adam/mq/
│   ├── common/                    # Common utilities
│   │   ├── Request.java          # Network request object
│   │   ├── Response.java         # Network response object
│   │   ├── Consumer.java         # Consumer callback interface
│   │   ├── BinaryTool.java       # Serialization utilities
│   │   └── *Arguments.java       # API parameter classes
│   │
│   ├── mqserver/                  # MQ Server core
│   │   ├── BrokerServer.java     # TCP server, protocol parsing
│   │   ├── VirtualHost.java      # Virtual host, core API
│   │   │
│   │   ├── core/                 # Domain models
│   │   │   ├── Exchange.java     # Exchange entity
│   │   │   ├── MSGQueue.java     # Queue entity
│   │   │   ├── Binding.java      # Binding relationship
│   │   │   ├── Message.java      # Message entity
│   │   │   ├── Router.java       # Routing engine
│   │   │   └── ConsumerManager.java  # Consumer management
│   │   │
│   │   ├── datacenter/           # Storage layer
│   │   │   ├── MemoryDataCenter.java   # In-memory storage
│   │   │   ├── DiskDataCenter.java     # Disk persistence
│   │   │   ├── DataBaseManager.java    # SQLite management
│   │   │   └── MessageFileManager.java # Message file storage
│   │   │
│   │   └── mapper/               # MyBatis mappers
│   │       └── MetaMapper.java   # Metadata persistence
│   │
│   └── mqclient/                  # Client SDK
│       ├── Connection.java       # Connection management
│       └── Channel.java          # Channel operations
│
├── docs/                          # Documentation
│   ├── RESTART_GUIDE.md          # Learning guide
│   ├── notes_fixed.md            # Study notes
│   └── 板书/                      # Course diagrams
│
└── pom.xml                        # Maven configuration
```

## Implemented Features

### Core Features

| Feature | Status | Description |
|---------|--------|-------------|
| Exchange Management | ✅ | Create/Delete exchanges (Direct, Fanout, Topic) |
| Queue Management | ✅ | Create/Delete queues with durability options |
| Binding Management | ✅ | Bindind/Unbinding queues to exchanges |
| Message Publishing | ✅ | Publish messages with routing keys |
| Message Subscription | ✅ | Subscribe to queues with push model |
| Message ACK | ✅ | Manual/Auto acknowledgment mechanism |

### Exchange Types

| Type | Routing Rule | Use Case |
|------|--------------|----------|
| **Direct** | Exact match RoutingKey == BindingKey | Point-to-point messaging |
| **Fanout** | Broadcast to all bound queues | System notifications |
| **Topic** | Pattern matching with `*` and `#` wildcards | Flexible subscription |

### Network Protocol

The server implements a binary protocol with 12 operation types:

| Code | Operation | Description |
|------|-----------|-------------|
| 0x1 | createChannel | Create a new channel |
| 0x2 | closeChannel | Close a channel |
| 0x3 | exchangeDeclare | Declare an exchange |
| 0x4 | exchangeDelete | Delete an exchange |
| 0x5 | queueDeclare | Declare a queue |
| 0x6 | queueDelete | Delete a queue |
| 0x7 | queueBind | Bind queue to exchange |
| 0x8 | queueUnbind | Unbind queue from exchange |
| 0x9 | basicPublish | Publish a message |
| 0xa | basicConsume | Subscribe to a queue |
| 0xb | basicAck | Acknowledge a message |
| 0xc | (Response) | Server push to consumer |

### Storage Architecture

- **Memory Layer**: ConcurrentHashMap for high-performance read/write
- **Disk Layer**: SQLite for metadata + Binary files for message bodies
- **GC Mechanism**: Copy-based garbage collection for message files

## Quick Start

### Prerequisites

- JDK 17 or higher
- Maven 3.6+

### Build

```bash
# Build the project
./mvnw clean package

# Run tests
./mvnw test
```

### Run

```bash
# Run the application
./mvnw spring-boot:run
```

## Documentation

- [Learning Guide](docs/RESTART_GUIDE.md) - Comprehensive study guide with architecture diagrams
- [Study Notes](docs/notes_fixed.md) - Detailed implementation notes
- [Course Material](docs/course.pdf) - Original course PDF

## License

Educational project - free to use and modify.

---

**Note**: This is a learning project created for educational purposes, implementing core message queue concepts inspired by RabbitMQ.
