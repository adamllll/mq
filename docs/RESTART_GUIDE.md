# 消息队列 (MQ) 学习手册

> **创建日期**: 2026-01-15
> **目标**: 从零开始理解消息队列核心原理，并掌握本项目的实现细节。

---

## 目录

1. [消息队列基础理论](#1-消息队列基础理论)
2. [项目概览](#2-项目概览)
3. [当前进度盘点](#3-当前进度盘点)
4. [核心架构图](#4-核心架构图)
5. [深度技术解析](#5-深度技术解析)
6. [关键代码路径导航](#6-关键代码路径导航)
7. [后续开发计划](#7-后续开发计划)
8. [学习检验与思考题](#8-学习检验与思考题)
9. [延伸阅读与参考资料](#9-延伸阅读与参考资料)

---

## 1. 消息队列基础理论

在深入代码之前，先建立对消息队列的宏观理解。

### 1.1 为什么需要消息队列？

在分布式系统中，服务之间的直接调用会带来三个核心问题：

| 问题 | 场景 | MQ 如何解决 |
| :--- | :--- | :--- |
| **耦合性高** | 服务 A 调用服务 B，B 挂了 A 也跟着出错 | 生产者只管发消息，不关心谁消费、是否在线 |
| **流量峰值** | 秒杀场景瞬间涌入 10 万请求，数据库被打崩 | 消息堆积在队列中，消费者按自身能力慢慢处理（削峰填谷） |
| **同步阻塞** | 用户下单后要等支付、库存、物流全部完成才响应 | 异步处理，下单后立即返回，后续流程由 MQ 驱动 |

**一句话总结**：MQ 是分布式系统的"缓冲区"和"解耦器"。

### 1.2 核心概念速查表

| 术语 | 英文 | 解释 | 类比 |
| :--- | :--- | :--- | :--- |
| **生产者** | Producer | 发送消息的应用程序 | 寄信人 |
| **消费者** | Consumer | 接收并处理消息的应用程序 | 收信人 |
| **队列** | Queue | 消息的存储容器，FIFO 结构 | 邮筒 |
| **交换机** | Exchange | 消息的路由中心，决定消息去哪个队列 | 邮局分拣中心 |
| **绑定** | Binding | 交换机与队列的关联规则 | 邮政编码规则 |
| **路由键** | RoutingKey | 生产者发送时附带的标签 | 信封上的地址 |
| **绑定键** | BindingKey | 队列订阅时声明的匹配模式 | 邮筒接受的地址范围 |
| **虚拟主机** | VirtualHost | 逻辑隔离的 MQ 实例 | 不同楼层的邮局 |
| **确认机制** | ACK | 消费者告知 MQ "我处理完了" | 签收回执 |

### 1.3 AMQP 协议简介

本项目设计思路参考了 **RabbitMQ**，而 RabbitMQ 是 AMQP (Advanced Message Queuing Protocol) 的标准实现。

AMQP 定义了三个核心组件的交互方式：

```
Producer → Exchange → Binding → Queue → Consumer
             ↓
        (路由规则)
```

**AMQP 的工作流程：**
1. 生产者将消息发送给 Exchange，并附带 RoutingKey
2. Exchange 根据类型和 BindingKey 匹配规则，决定转发给哪些 Queue
3. 消费者从 Queue 中拉取或接收推送的消息
4. 消费完成后发送 ACK，MQ 才会删除该消息

### 1.4 三种交换机模式对比

| 类型 | 匹配规则 | 使用场景 | 示例 |
| :--- | :--- | :--- | :--- |
| **Direct** | RoutingKey == BindingKey (精确匹配) | 点对点通信，如日志分级 | `routingKey="error"` 只进 error 队列 |
| **Fanout** | 无视 Key，广播给所有绑定队列 | 广播通知，如系统公告 | 所有服务都收到同一条消息 |
| **Topic** | 支持通配符 `*` 和 `#` 的模式匹配 | 灵活订阅，如新闻分类 | `usa.*` 匹配 `usa.news`、`usa.weather` |

**Topic 通配符规则：**
- `*`：匹配**恰好一个**单词。如 `order.*` 可匹配 `order.created`，不能匹配 `order.item.created`
- `#`：匹配**零个或多个**单词。如 `order.#` 可匹配 `order`、`order.created`、`order.item.created`

---

## 2. 项目概览

本项目是一个基于 **Java 17** 和 **Spring Boot** 开发的轻量级消息队列，设计思路参考了 **RabbitMQ**。

### 2.1 核心设计理念

- **生产者-消费者模型**：实现服务解耦与异步通信
- **双层存储**：内存保证速度，硬盘保证持久化
- **多虚拟主机**：逻辑隔离，代码结构已支持（目前使用默认 Host）

### 2.2 关键特性

- ✅ 支持 **Direct**, **Fanout**, **Topic** 三种交换机模式
- ✅ 支持 **持久化** (SQLite + 磁盘文件) 与 **内存** 双层存储
- ✅ 支持 **消息确认机制 (ACK)**
- ✅ 支持消息文件的 **GC 垃圾回收**

---

## 3. 当前进度盘点

截止目前，**核心业务逻辑层 (Core Domain)** 已基本完成，**网络通信层**尚未开始。

### ✅ 已完成模块 (Server Core)

| 模块 | 核心类 | 功能描述 |
| :--- | :--- | :--- |
| **核心模型** | `Exchange`, `MSGQueue`, `Binding`, `Message` | 定义了 MQ 的基本概念实体 |
| **路由引擎** | `Router` | 实现消息从 Exchange 到 Queue 的匹配规则 (含 Topic 通配符匹配) |
| **持久化层** | `DiskDataCenter`, `DataBaseManager`, `MessageFileManager` | SQLite 存储元数据，文件系统存储消息体 |
| **内存层** | `MemoryDataCenter` | 维护内存中的状态 (ConcurrentHashMap)，提供高性能读写 |
| **控制中心** | `VirtualHost` | 整合内存与硬盘管理，对外提供核心 API |
| **消费管理** | `ConsumerManager` | 维护消费者订阅关系，实现消息的轮询推送 |

### 🚧 待开发模块 (Network & Client)

| 模块 | 缺失内容 | 计划 |
| :--- | :--- | :--- |
| **通信协议** | `Request`, `Response` 对象 | 定义客户端与服务端交互的二进制协议格式 |
| **服务端网络** | `BrokerServer` | 基于 Socket/Netty 实现 TCP 监听，解析协议并调用 VirtualHost |
| **客户端 SDK** | `Connection`, `Channel` | 封装网络通信，给用户提供易用的发送/订阅接口 |

---

## 4. 核心架构图

### 4.1 整体架构

```mermaid
graph TD
    User[用户/开发者] --> ClientSDK[客户端 SDK - 待开发]

    subgraph "MQ Server (服务端)"
        BrokerServer[BrokerServer - 待开发]

        subgraph "VirtualHost (虚拟主机)"
            Router[Router 路由匹配]
            CM[ConsumerManager 消费管理]

            subgraph "Storage Engine"
                MDC[MemoryDataCenter - 内存]
                DDC[DiskDataCenter - 硬盘]
            end
        end
    end

    ClientSDK <-->|TCP / 自定义协议| BrokerServer
    BrokerServer -->|调用 API| VirtualHost

    VirtualHost --> Router
    VirtualHost --> CM
    VirtualHost --> MDC
    VirtualHost --> DDC

    MDC <-->|恢复/刷盘| DDC
    DDC -->|元数据| SQLite[(SQLite DB)]
    DDC -->|消息内容| FileSys[文件系统]
```

### 4.2 消息流转时序图

```mermaid
sequenceDiagram
    participant P as Producer
    participant VH as VirtualHost
    participant R as Router
    participant DDC as DiskDataCenter
    participant MDC as MemoryDataCenter
    participant CM as ConsumerManager
    participant C as Consumer

    P->>VH: basicPublish(exchange, routingKey, message)
    VH->>R: route(exchangeType, binding, message)
    R-->>VH: 返回目标队列列表

    loop 每个目标队列
        VH->>DDC: sendMessage(queue, message) [如果持久化]
        VH->>MDC: sendMessage(queue, message)
        VH->>CM: notifyConsume(queueName)
    end

    CM->>MDC: pollMessage(queueName)
    MDC-->>CM: message
    CM->>C: callback.handleDelivery(message)
    C-->>CM: ACK
    CM->>DDC: deleteMessage() [逻辑删除]
    CM->>MDC: removeMessage()
```

---

## 5. 深度技术解析

### 5.1 Exchange 路由机制

```mermaid
graph LR
    P[生产者] -->|Msg: routingKey='order.new'| E{Exchange}

    subgraph "Direct 模式"
        E -- key='order.new' --> Q1[Queue A]
        E -- key='order.pay' --x Q1
    end

    subgraph "Fanout 模式"
        E --> Q2[Queue B]
        E --> Q3[Queue C]
    end

    subgraph "Topic 模式"
        E -- key='*.new' --> Q4[Queue D]
        E -- key='order.#' --> Q5[Queue E]
    end
```

### 5.2 Topic 通配符匹配算法 ⭐

这是本项目的核心算法之一，使用**双指针遍历**实现模式匹配。

#### 算法思路

将 `bindingKey` 和 `routingKey` 按 `.` 分割成 token 数组，用两个指针分别遍历：

```
bindingKey: "order.#.completed"  →  ["order", "#", "completed"]
routingKey: "order.item.payment.completed"  →  ["order", "item", "payment", "completed"]
```

#### 核心代码解析

```java
// 文件位置: src/main/java/org/adam/mq/mqserver/core/Router.java

private boolean routeTopic(Binding binding, Message message) {
    String[] bindingTokens = binding.getBindingKey().split("\\.");
    String[] routingTokens = message.getRoutingKey().split("\\.");

    int bindingIndex = 0;
    int routingIndex = 0;

    while (bindingIndex < bindingTokens.length && routingIndex < routingTokens.length) {
        if (bindingTokens[bindingIndex].equals("*")) {
            // 情况1: * 匹配任意单个词，双方各前进一步
            bindingIndex++;
            routingIndex++;
        } else if (bindingTokens[bindingIndex].equals("#")) {
            bindingIndex++;
            if (bindingIndex == bindingTokens.length) {
                // 情况2: # 在末尾，直接匹配成功（吞掉剩余所有词）
                return true;
            }
            // 情况3: # 不在末尾，需要在 routingTokens 中找下一个匹配点
            routingIndex = findNextMatch(routingTokens, routingIndex, bindingTokens[bindingIndex]);
            if (routingIndex == -1) {
                return false;  // 找不到匹配，失败
            }
            bindingIndex++;
            routingIndex++;
        } else {
            // 情况4: 普通字符串，要求完全相等
            if (!bindingTokens[bindingIndex].equals(routingTokens[routingIndex])) {
                return false;
            }
            bindingIndex++;
            routingIndex++;
        }
    }
    // 情况5: 只有双方都走到末尾才算匹配成功
    return bindingIndex == bindingTokens.length && routingIndex == routingTokens.length;
}
```

#### 匹配测试用例

| BindingKey | RoutingKey | 结果 | 说明 |
| :--- | :--- | :--- | :--- |
| `aaa.bbb` | `aaa.bbb` | ✅ true | 精确匹配 |
| `aaa.*` | `aaa.bbb` | ✅ true | `*` 匹配 `bbb` |
| `aaa.*.ccc` | `aaa.bbb.ccc` | ✅ true | `*` 匹配中间的 `bbb` |
| `aaa.#` | `aaa.bbb.ccc` | ✅ true | `#` 吞掉 `bbb.ccc` |
| `aaa.#.ccc` | `aaa.bbb.ccc` | ✅ true | `#` 匹配 `bbb`，然后精确匹配 `ccc` |
| `aaa.#.ccc` | `aaa.ccc` | ✅ true | `#` 匹配零个词 |
| `aaa.bbb` | `aaa.bbb.ccc` | ❌ false | 长度不匹配 |
| `*.aaa` | `aaa` | ❌ false | `*` 必须匹配恰好一个词 |

### 5.3 双层存储架构

我们采用 **内存 + 硬盘** 的混合存储策略，以平衡性能与数据安全性。

| 存储层 | 介质 | 存储内容 | 目的 |
| :--- | :--- | :--- | :--- |
| **内存层** | RAM (ConcurrentHashMap) | Exchange, Queue, Binding, Message | 极低延迟的读写操作 |
| **硬盘层 - DB** | SQLite | Exchange, Queue, Binding 元数据 | 结构化数据持久化 |
| **硬盘层 - File** | Binary Files | Message 消息体 | 大量变长数据的高效存储 |

#### 为什么消息体不存数据库？

消息是"流数据"，特点是量大、变长、生命周期短。相比数据库的随机写入，文件系统的**顺序追加写 (Sequential Write)** 效率高出数量级，且更容易做 GC。

### 5.4 消息文件存储格式 ⭐

消息以**变长二进制格式**追加存储在 `queue_data.txt` 文件中：

```
┌──────────────┬────────────────────────────────┐
│  4 bytes     │         N bytes                │
│  消息长度    │         消息体 (序列化)         │
├──────────────┼────────────────────────────────┤
│  4 bytes     │         N bytes                │
│  消息长度    │         消息体 (序列化)         │
└──────────────┴────────────────────────────────┘
```

#### 关键字段

每条 `Message` 对象包含：
- `offsetBeg`: 消息体在文件中的起始偏移量（不含长度字段）
- `offsetEnd`: 消息体在文件中的结束偏移量
- `isValid`: 有效标记（0x1 有效，0x0 已删除）

#### 核心代码解析

```java
// 文件位置: src/main/java/org/adam/mq/mqserver/datacenter/MessageFileManager.java

public void sendMessage(MSGQueue queue, Message message) throws MqException, IOException {
    byte[] messageBinary = BinaryTool.toBytes(message);  // 序列化

    synchronized (queue) {  // 以队列粒度加锁，保证线程安全
        try (RandomAccessFile raf = new RandomAccessFile(queueDataFile, "rw")) {
            long fileLength = raf.length();
            raf.seek(fileLength);  // 移动到文件末尾

            // 记录消息在文件中的位置（用于后续删除）
            message.setOffsetBeg(fileLength + 4);  // +4 跳过长度字段
            message.setOffsetEnd(fileLength + 4 + messageBinary.length);

            raf.writeInt(messageBinary.length);  // 写入长度
            raf.write(messageBinary);            // 写入消息体
        }
        // 更新统计信息
        Stat stat = readStat(queue.getName());
        stat.totalCount += 1;
        stat.validCount += 1;
        writeStat(queue.getName(), stat);
    }
}
```

### 5.5 GC 垃圾回收机制 ⭐

消息被消费后采用**逻辑删除**（仅修改 `isValid` 标记），随着时间推移会产生大量"垃圾"。GC 机制负责清理这些无效数据。

#### 触发条件

```java
public boolean checkGC(String queueName) {
    Stat stat = readStat(queueName);
    // 总消息数 > 2000 且 有效率 < 50%
    return stat.totalCount > 2000 &&
           (double)stat.validCount / stat.totalCount < 0.5;
}
```

#### GC 算法：复制算法

与 JVM 新生代 GC 类似，采用**复制算法**：

```
1. 创建新文件 queue_data_new.txt
2. 遍历原文件，只把 isValid=1 的消息复制到新文件
3. 删除原文件 queue_data.txt
4. 重命名 queue_data_new.txt → queue_data.txt
5. 更新统计文件
```

```mermaid
graph LR
    subgraph "GC 前"
        A[Msg1 ✓] --> B[Msg2 ✗]
        B --> C[Msg3 ✓]
        C --> D[Msg4 ✗]
        D --> E[Msg5 ✓]
    end

    subgraph "GC 后"
        F[Msg1 ✓] --> G[Msg3 ✓]
        G --> H[Msg5 ✓]
    end
```

### 5.6 线程安全设计

本项目在多个层面保证线程安全：

| 层级 | 保护对象 | 机制 |
| :--- | :--- | :--- |
| VirtualHost | Exchange 操作 | `synchronized (exchangeLocker)` |
| VirtualHost | Queue 操作 | `synchronized (queueLocker)` |
| MessageFileManager | 单个队列的文件读写 | `synchronized (queue)` |
| MemoryDataCenter | 数据结构 | `ConcurrentHashMap` |

**嵌套锁的注意事项**：在 `queueBind` 操作中，需要同时操作交换机和队列，此时采用固定的加锁顺序（先 exchangeLocker 再 queueLocker）避免死锁。

---

## 6. 关键代码路径导航

建议按以下顺序阅读代码，从上层到底层：

### 第一阶段：理解核心 API

1. **VirtualHost.java** - 业务逻辑入口
   - 路径: `src/main/java/org/adam/mq/mqserver/VirtualHost.java`
   - 重点方法: `exchangeDeclare()`, `queueDeclare()`, `basicPublish()`, `basicConsume()`
   - 理解: 如何协调内存层和硬盘层

### 第二阶段：理解存储层

2. **MemoryDataCenter.java** - 内存数据管理
   - 路径: `src/main/java/org/adam/mq/mqserver/datacenter/MemoryDataCenter.java`
   - 重点: ConcurrentHashMap 的使用，`recovery()` 数据恢复

3. **DiskDataCenter.java** - 硬盘数据管理
   - 路径: `src/main/java/org/adam/mq/mqserver/datacenter/DiskDataCenter.java`
   - 重点: `init()` 初始化流程，`sendMessage()` 持久化逻辑

4. **MessageFileManager.java** - 消息文件管理
   - 路径: `src/main/java/org/adam/mq/mqserver/datacenter/MessageFileManager.java`
   - 重点: `sendMessage()`, `deleteMessage()`, `gc()`, 文件格式设计

### 第三阶段：理解路由与消费

5. **Router.java** - 路由匹配算法
   - 路径: `src/main/java/org/adam/mq/mqserver/core/Router.java`
   - 重点: `routeTopic()` 通配符匹配算法

6. **ConsumerManager.java** - 消费者管理
   - 路径: `src/main/java/org/adam/mq/mqserver/core/ConsumerManager.java`
   - 重点: 消费者轮询机制，消息推送流程

---

## 7. 后续开发计划

按照以下步骤完成剩余开发：

```mermaid
graph LR
    A[阶段一: 协议定义] --> B[阶段二: 服务端网络]
    B --> C[阶段三: 客户端 SDK]
    C --> D[阶段四: 联调演示]
```

### 阶段一：协议定义
- 在 `common` 包下定义 `Request` 和 `Response`
- 设计二进制协议格式，统一前后端"语言"

### 阶段二：服务端网络接入
- 实现 `BrokerServer`，基于 Socket 或 Netty
- 解析请求协议，调用 VirtualHost API

### 阶段三：客户端 SDK
- 编写 `Connection` 和 `Channel`
- 封装网络通信，提供易用的发送/订阅接口

### 阶段四：联调与演示
- 编写 Demo 程序
- 跑通"生产 → 存储 → 消费"全流程

---

## 8. 学习检验与思考题

完成以下问题，检验你对项目的理解程度。

### 🟢 基础题

1. **概念辨析**：RoutingKey 和 BindingKey 有什么区别？分别在什么时候使用？

2. **模式选择**：如果你需要实现一个"所有服务都收到同一条系统通知"的功能，应该使用哪种交换机类型？

3. **存储设计**：为什么元数据存 SQLite，而消息体存文件？如果反过来会有什么问题？

### 🟡 进阶题

4. **算法分析**：`routeTopic()` 方法的时间复杂度是多少？能否优化？

5. **GC 策略**：当前的 GC 触发条件是 `totalCount > 2000 && validRate < 0.5`，这个设计有什么优缺点？你会如何改进？

6. **线程安全**：为什么 VirtualHost 要使用两把不同的锁（exchangeLocker 和 queueLocker）而不是一把全局锁？

### 🔴 挑战题

7. **故障恢复**：如果消息写入文件成功，但更新统计文件时服务器崩溃了，会发生什么？如何解决这个问题？

8. **性能优化**：如果每秒有 10 万条消息写入同一个队列，当前的 `synchronized (queue)` 锁会成为瓶颈吗？你会如何优化？

9. **功能扩展**：如果要实现"延迟消息"功能（消息在指定时间后才能被消费），你会如何设计？

---

## 9. 延伸阅读与参考资料

### 官方文档

- [RabbitMQ 官方文档](https://www.rabbitmq.com/documentation.html) - 本项目的设计参考
- [AMQP 0-9-1 协议规范](https://www.rabbitmq.com/amqp-0-9-1-reference.html) - 理解消息队列的协议层

### 技术文章

- [消息队列设计精要](https://tech.meituan.com/2016/07/01/mq-design.html) - 美团技术团队的 MQ 设计思考
- [RabbitMQ 消息可靠性投递](https://www.cnblogs.com/vipstone/p/9350075.html) - 理解 ACK 机制

### 设计模式与原理

- **生产者-消费者模式**：本项目的核心架构模式
- **发布-订阅模式**：Fanout 和 Topic 交换机的理论基础
- **策略模式**：Router 根据 ExchangeType 选择不同路由策略

### 相关开源项目

- [RabbitMQ](https://github.com/rabbitmq/rabbitmq-server) - Erlang 实现的工业级 MQ
- [Apache RocketMQ](https://github.com/apache/rocketmq) - 阿里开源的分布式消息中间件
- [Apache Kafka](https://github.com/apache/kafka) - 高吞吐量的分布式事件流平台

---

*文档生成于：2026-01-15*
*祝学习愉快！遇到问题先翻代码，代码不会骗你。*
