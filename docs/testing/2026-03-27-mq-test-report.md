# MQ 项目测试报告

## 1. 项目背景与测试目标

MQ 是一个基于 Java 17、Spring Boot 3.5.8、SQLite 和自定义 TCP 协议实现的学习型消息队列项目，支持 Exchange / Queue 路由、消息持久化、发布 / 消费 / ACK 全链路，以及客户端 SDK。

本轮测试工作的目标不是单纯“把用例数堆上去”，而是围绕以下问题建立可复跑、可解释、可追溯的测试资产：

- 路由规则是否能稳定拦截非法 `bindingKey` / `routingKey`
- 消息从发布到消费再到 ACK 删除的状态转换是否一致
- 持久化层在 SQLite 与消息文件并存的前提下是否具备基本正确性
- 客户端 SDK 与 Broker 的真实 TCP 链路是否能跑通
- 性能方面能否给出一组最基本、可复跑的 smoke 基线

本轮测试方案设计、用例补齐、测试基础设施抽取和报告整理均由本人负责完成。

## 2. 测试范围与环境

### 2.1 测试范围

| 范围 | 内容 | 是否纳入日常回归 |
| --- | --- | --- |
| 单元测试 | `RouterTests`、`MessageFileManagerTest`、`MemoryDataCenterTests` | 是 |
| 集成测试 | `VirtualHostTest`、`DiskDataCenterTests`、`DataBaseManagerTests` | 是 |
| 端到端测试 | `MqClientTests` | 是 |
| 启动 smoke | `MqApplicationTests` | 是 |
| 性能 smoke | `MqPerformanceTests` 的 3 个场景 | 否，手动 gated |

### 2.2 执行命令

- 自动化回归：`.\mvnw.cmd clean test`
- 性能 smoke：`.\mvnw.cmd "-Dmq.perf.enabled=true" "-Dtest=MqPerformanceTests" test`

说明：

- 回归套件统计口径为 8 个测试类、77 个用例。
- 性能 smoke 独立执行，不计入 77 个回归用例，也不作为覆盖率统计输入。

### 2.3 测试环境

| 项目 | 配置 |
| --- | --- |
| JDK | 17 |
| Spring Boot | 3.5.8 |
| SQLite JDBC | 3.46.0.0 |
| 测试框架 | JUnit 5 (Jupiter) |
| 覆盖率工具 | JaCoCo 0.8.12 |
| 操作系统 | Windows 11 / PowerShell |
| 构建工具 | Maven Wrapper |

## 3. 测试策略与用例设计方法

### 3.1 分层策略

| 层次 | 聚焦对象 | 设计理由 |
| --- | --- | --- |
| 单元层 | Router、MessageFileManager、MemoryDataCenter | 逻辑纯、边界多，适合高频快速回归 |
| 集成层 | VirtualHost、DiskDataCenter、DataBaseManager | 依赖 SQLite 与文件系统，mock 无法覆盖真实持久化行为 |
| 端到端层 | MqClient + BrokerServer | 需要验证自定义 TCP 协议、Channel 复用和 Consumer 回调链路 |
| 性能层 | 吞吐与延迟 smoke | 为项目建立一组可解释的性能基线，而非工业级压测结论 |

### 3.2 用例设计方法

| 方法 | 应用场景 | 具体实践 |
| --- | --- | --- |
| 等价类划分 | Router TOPIC 匹配 | 将 `bindingKey` 分为合法模式与非法模式，例如 `aaa.*` 与 `order.a*` |
| 边界值分析 | MessageFileManager GC | 验证 `totalCount > 2000` 与 `validRatio < 0.5` 的边界点 |
| 状态转换 | VirtualHost 消息生命周期 | 覆盖 `publish -> consume -> ACK -> durable 清理` 完整链路 |
| 异常路径回归 | 缺失资源、非法输入 | 覆盖缺失 exchange、缺失消息 ACK、非法 routingKey 等负路径 |

### 3.3 为什么集成层不用 mock

本项目的持久化链路同时依赖 SQLite 元数据和自定义消息文件。若在集成层大量使用 mock，会直接跳过以下高风险问题：

- SQLite 文件锁与上下文释放顺序问题
- 消息文件读写与删除的一致性问题
- `@BeforeEach` / `@AfterEach` 数据清理不彻底导致的测试污染

因此，集成层明确采用真实 SQLite 实例与真实文件系统，测试重点不是“接口调没调用”，而是“状态最终是否一致”。

## 4. 测试基础设施与数据隔离

### 4.1 TestRuntimeSupport 的职责

| 能力 | 作用 |
| --- | --- |
| `startApplicationContext()` / `stopApplicationContext()` | 统一管理 Spring 上下文生命周期 |
| `deleteDataDirectory()` | 清理 `./data`，避免 SQLite 与消息文件残留互相污染 |
| `startBroker()` / `awaitBrokerReady()` | 在测试内启动 Broker，并基于 Socket 探测等待端口就绪 |
| `awaitThreadStopped()` | teardown 时确认 Broker 测试线程已退出 |
| `assertConditionStaysTrue()` | 负面异步验证时，用条件稳定等待替代盲目 `sleep` |

### 4.2 数据隔离策略

- `@BeforeEach` 先清理 `./data`，再启动上下文，保证每个测试从干净状态开始。
- `@AfterEach` 先停 Spring / Broker，再删目录，避免 Windows 下文件锁导致删除失败。
- 每个测试类都独立管理运行时资源，不依赖 `@SpringBootTest` 的上下文缓存。

### 4.3 异步测试可靠性方案

Consumer 回调运行在独立线程中，直接在回调线程里断言容易出现“断言失败被线程吞掉、主线程误判为通过”的问题。因此本轮异步测试统一采用以下模式：

1. 回调线程只负责写入 `AtomicReference` / `AtomicInteger`
2. 回调线程通过 `CountDownLatch` 通知主线程事件已发生
3. 主线程等待超时并做最终断言
4. 对负面异步场景，使用 `assertConditionStaysTrue()` 观察一段稳定窗口，确认没有额外消息投递

这套模式已经应用到 `MqClientTests` 与 `VirtualHostTest` 的异步消费、ACK 和 topic 负面验证场景中。

## 5. 核心测试场景说明

### 5.1 测试矩阵

| 模块 | 测试类 | 用例数 | 重点 |
| --- | --- | --- | --- |
| 路由规则 | `RouterTests` | 19 | TOPIC 通配符匹配、非法 `bindingKey` 拦截、空交换机类型 |
| 虚拟主机 | `VirtualHostTest` | 17 | 发布 / 消费 / ACK 生命周期、durable ACK 清理、负路径回归 |
| 客户端链路 | `MqClientTests` | 8 | 真实 TCP 链路、单连接多 Channel、重复 Consumer 限制 |
| 文件存储 | `MessageFileManagerTest` | 8 | GC 阈值边界、消息文件读写删除 |
| 磁盘数据中心 | `DiskDataCenterTests` | 10 | 元数据与消息落盘协同 |
| 数据库管理 | `DataBaseManagerTests` | 7 | Exchange / Queue / Binding 元数据 CRUD |
| 内存数据中心 | `MemoryDataCenterTests` | 7 | 内存态对象与恢复 |
| 应用启动 | `MqApplicationTests` | 1 | Spring Boot 启动 smoke |

### 5.2 代表性场景

**Router：非法通配符校验**

- 正例：`aaa.*`、`aaa.#`
- 反例：`order.a*`、`order.#suffix`、`order.#.*.created`
- 目标：验证 `*` / `#` 必须作为独立 token 出现

**VirtualHost：TOPIC 模式负面验证**

- `bindingKey = user.*.update`
- 发布一条匹配消息和一条不匹配消息
- 使用 `CountDownLatch` 等待匹配消息到达
- 使用条件稳定等待确认不匹配消息未被投递

**VirtualHost：durable ACK 清理**

- 发布 `deliveryMode = 2` 的持久化消息
- Consumer 手动 ACK
- 验证消息从待确认结构与磁盘消息文件中都被移除

**MqClient：单连接多 Channel**

- 同一连接上创建两个 Channel
- 验证 `channelId` 不同
- 确认客户端支持基本的 AMQP 风格多路复用语义

## 6. 缺陷发现与根因分析

### 6.1 缺陷现象

在补写 Router 负路径用例时，发现 `Router.checkBindingKey()` 会错误放行 `order.a*`、`order.#suffix` 这类非法模式。

### 6.2 根因

原实现只检查了相邻通配符组合，例如：

- `#.#`
- `#.*`
- `*.#`

但没有检查“通配符是否嵌入 token 内部”。因此 `a*` 这种长度大于 1 且包含 `*` 的 token 会被误判为合法。

### 6.3 修复策略

- 在 `bindingKey` 按 `.` 切分后，新增一轮 token 级校验
- 若 token 长度大于 1 且包含 `*` 或 `#`，直接判非法
- 保留原有相邻通配符限制逻辑不变，避免扩大改动面

### 6.4 回归验证

- `RouterTests` 扩展后为 19 个用例，全部通过
- `VirtualHostTest` 同步补充非法 `bindingKey` / `routingKey` 的链路级回归
- 证明校验不仅在 Router 层正确，也在上层调用链中被实际覆盖

## 7. 自动化执行结果

### 7.1 回归结果

| 测试类 | 用例数 | 结果 | 耗时 |
| --- | --- | --- | --- |
| `RouterTests` | 19 | 通过 | 0.085 s |
| `VirtualHostTest` | 17 | 通过 | 12.02 s |
| `MqClientTests` | 8 | 通过 | 6.216 s |
| `MessageFileManagerTest` | 8 | 通过 | 0.764 s |
| `DiskDataCenterTests` | 10 | 通过 | 12.20 s |
| `DataBaseManagerTests` | 7 | 通过 | 13.90 s |
| `MemoryDataCenterTests` | 7 | 通过 | 0.845 s |
| `MqApplicationTests` | 1 | 通过 | 0.013 s |
| **合计** | **77** | **77/77 通过** | **46.04 s** |

### 7.2 执行结论

- 当前日常回归套件通过率为 100%
- 回归速度控制在 1 分钟内，适合作为本地开发与 CI 基线
- 性能 smoke 已通过系统属性隔离，不影响 `clean test` 的日常执行体验

## 8. 覆盖率与质量评估

### 8.1 覆盖率结果

| 指标 | 数值 |
| --- | --- |
| 行覆盖率 | 84.60% |
| 已覆盖行 | 1242 |
| 未覆盖行 | 226 |
| 总行数 | 1468 |

### 8.2 与目标对比

| 目标 | 实际 | 判断 |
| --- | --- | --- |
| 核心业务路径具备自动化保护 | 已达成 | 路由、发布消费 ACK、持久化与客户端链路均有回归保护 |
| 行覆盖率大于 85% | 84.60% | 接近目标，但尚未完全达成 |
| 建立可复跑性能基线 | 已达成 | 3 个性能 smoke 场景可独立执行 |

### 8.3 主要未覆盖路径

- `MqApplication.main()`：测试直接管理上下文，不走生产入口
- `BrokerServer` 网络异常分支：需要故障 Socket 或异常关闭场景
- `ConsumerManager` 线程池中断 / 异常传播路径：需要专门的故障注入

### 8.4 质量判断

- 当前覆盖率已经足以支撑核心链路回归，但还不能把“84.60%”简单等同于“系统已经足够健壮”。
- 真正的薄弱点不在普通正例，而在网络异常、线程中断、Broker 重启恢复等韧性场景。

## 9. 性能 smoke 结果与解读

### 9.1 执行说明

性能 smoke 通过独立命令执行，不纳入日常回归，也不与覆盖率统计混算。本报告记录的是一轮独立 perf run 的结果，用于说明趋势和瓶颈，不作为工业级基准。

### 9.2 结果表

| 场景 | 线程数 | 消息量 | 成功 | 失败 | 耗时(ms) | TPS | 平均耗时(ms/条) |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `single-producer` | 1 | 500 | 500 | 0 | 347.81 | 1437.55 | 0.70 |
| `multi-producer` | 4 | 1000 | 1000 | 0 | 200.92 | 4977.18 | 0.20 |
| `publish-consume-ack` | 1 | 100 | 100 | 0 | 427.67 | 233.83 | 4.28 |

### 9.3 数据解读

- `multi-producer` 的 TPS 是 `single-producer` 的约 `3.46` 倍，说明并发发布具备明显扩展性，但并未达到完全线性扩展。
- 没有达到接近 4 倍的原因主要有两类：
  - Broker 端仍存在共享资源竞争，例如内存结构、持久化写入与线程切换成本
  - 单次测试规模较小，连接建立、调度和 JVM 抖动会放大波动
- `publish-consume-ack` 的平均耗时为 `4.28 ms/条`，约为纯发布场景的 `6.11` 倍。瓶颈主要来自：
  - `publish -> consume -> ACK` 完整状态链路
  - durable 消息的磁盘写入与删除
  - Consumer 回调线程调度与 ACK 同步确认

### 9.4 结果边界

- 这组数据适合作为 smoke baseline，不适合作为容量规划依据。
- 如需进一步判断系统上限，需要提高消息量、延长运行时间，并引入更严格的并发与故障场景。

## 10. 风险、局限与后续优化

### 10.1 当前风险

- 缺少网络故障注入，`BrokerServer` 的异常处理分支覆盖不足
- 缺少 Broker 重启恢复验证，无法证明 SQLite + 消息文件的恢复链路
- 缺少长稳测试，尚未验证内存、文件句柄与线程资源在长时间运行下的表现
- 负路径虽然已覆盖，但业务代码仍会打印堆栈，日志可读性一般

### 10.2 后续优化优先级

| 方向 | 价值 | 优先级 |
| --- | --- | --- |
| Broker 重启恢复测试 | 直接补齐持久化恢复可信度 | 高 |
| 网络故障注入测试 | 覆盖异常 Socket / 超时 / 中断路径 | 高 |
| 长稳与高并发压测 | 评估资源泄漏与性能拐点 | 中 |
| 覆盖率按模块拆解 | 更精确定位薄弱点 | 中 |
| 负路径日志治理 | 提升问题定位效率 | 低 |

## 11. 结论

本轮测试工作已经完成以下目标：

- 建立了 77 个自动化回归用例，形成覆盖单元、集成、端到端和启动 smoke 的基础回归网
- 发现并推动修复了 Router 通配符校验缺陷，补齐了多类负路径用例
- 通过 `TestRuntimeSupport`、`CountDownLatch`、条件稳定等待等机制，提高了异步测试的可重复性与可信度
- 给出了 84.60% 的回归覆盖率和 3 个性能 smoke 场景的基线结果

如果以“能否支持后续持续迭代”为标准，这套测试资产已经具备实际价值；如果以“是否达到工业级韧性验证”为标准，网络故障、重启恢复和长稳压测仍然是下一阶段的重点。
