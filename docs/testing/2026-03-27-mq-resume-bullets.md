# MQ 项目简历亮点

- 为自研 Java MQ 系统设计并落地 77 个自动化回归用例（8 个测试类）与 3 个手动 gated 性能 smoke 场景，覆盖路由、持久化、客户端 TCP 链路和 ACK 核心路径。
- 引入 JaCoCo 与白名单统计口径，建立可追溯测试资产，回归套件行覆盖率达到 84.60%（1242/1468），并明确定位剩余盲区到网络异常和线程中断分支。
- 发现并推动修复 `bindingKey` 通配符校验漏洞，补齐非法 `bindingKey` / `routingKey`、缺失 exchange、缺失消息 ACK、durable ACK 清理等负路径回归。
- 抽象 `TestRuntimeSupport` 管理 Spring 上下文、Broker 生命周期和数据目录清理，异步场景采用 CountDownLatch + 主线程断言 + 条件稳定等待，提升测试稳定性与可重复性。
- 编写 Java 性能 smoke 脚本，得到单线程 1437.55 TPS、4 线程 4977.18 TPS、publish-consume-ack 平均 4.28 ms 的基线结果，为性能分析与面试答辩提供依据。
