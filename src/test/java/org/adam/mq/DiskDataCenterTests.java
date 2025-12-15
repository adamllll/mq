package org.adam.mq;

import org.adam.mq.common.MqException;
import org.adam.mq.mqserver.core.Binding;
import org.adam.mq.mqserver.core.Exchange;
import org.adam.mq.mqserver.core.ExchangeType;
import org.adam.mq.mqserver.core.MSGQueue;
import org.adam.mq.mqserver.core.Message;
import org.adam.mq.mqserver.datacenter.DiskDataCenter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * DiskDataCenter 单元测试类
 * 
 * 测试目标: 验证 DiskDataCenter 类的所有功能是否正常工作
 * 测试范围:
 * 1. 初始化功能测试
 * 2. 交换机操作测试 (增删查)
 * 3. 队列操作测试 (增删查)
 * 4. 绑定操作测试 (增删查)
 * 5. 消息操作测试 (发送、删除、加载)
 * 
 * 注意事项:
 * - 每个测试用例都是独立的,互不影响
 * - 使用 @BeforeEach 进行测试前的准备工作
 * - 使用 @AfterEach 进行测试后的清理工作
 * - 所有测试都在真实的文件系统上进行,确保测试的可靠性
 */
@SpringBootTest
public class DiskDataCenterTests {
    // 被测试的目标对象 - 硬盘数据中心管理器
    private DiskDataCenter diskDataCenter = new DiskDataCenter();

    /**
     * 每个测试用例执行前的准备工作
     * 
     * 主要任务:
     * 1. 初始化 Spring 应用上下文,确保依赖注入正常工作
     * 2. 调用 DiskDataCenter 的 init() 方法,初始化数据库和消息文件管理器
     * 
     * 为什么需要初始化 Spring 上下文?w
     * - DiskDataCenter 内部的 DataBaseManager 需要通过 Spring 获取 MetaMapper 实例
     * - MetaMapper 是通过 MyBatis 生成的,需要在 Spring 容器中管理
     */
    @BeforeEach
    public void setUp() {
        // 启动 Spring Boot 应用上下文,使得 Spring 容器中的 Bean 可以被正常使用
        MqApplication.context = SpringApplication.run(MqApplication.class);
        
        // 初始化 DiskDataCenter,这会同时初始化:
        // 1. 数据库 (创建表、插入默认交换机)
        // 2. 消息文件管理器 (当前为空实现,为后续扩展预留)
        diskDataCenter.init();
    }

    /**
     * 每个测试用例执行后的清理工作
     * 
     * 主要任务:
     * 1. 关闭 Spring 应用上下文,释放资源
     * 2. 删除测试过程中产生的数据库文件和数据目录
     * 
     * 为什么需要关闭上下文?
     * - 如果不关闭,数据库连接不会释放,导致数据库文件无法删除
     * - 占用的 8080 端口不会释放,影响下一个测试用例的执行
     * 
     * 为什么要删除数据库?
     * - 确保每个测试用例都在干净的环境下运行
     * - 避免测试用例之间的相互影响
     */
    @AfterEach
    public void tearDown() {
        // 关闭 Spring 上下文,释放数据库连接和端口
        MqApplication.context.close();
        
        // 删除测试数据库文件和 data 目录
        java.io.File dbFile = new java.io.File("./data/meta.db");
        if (dbFile.exists()) {
            dbFile.delete();
        }
        java.io.File dataDir = new java.io.File("./data");
        if (dataDir.exists()) {
            // 递归删除 data 目录下的所有文件和子目录
            deleteDirectory(dataDir);
        }
    }

    /**
     * 辅助方法: 递归删除目录及其所有内容
     * 
     * @param directory 要删除的目录
     */
    private void deleteDirectory(java.io.File directory) {
        if (directory.isDirectory()) {
            java.io.File[] files = directory.listFiles();
            if (files != null) {
                for (java.io.File file : files) {
                    deleteDirectory(file);
                }
            }
        }
        directory.delete();
    }

    /**
     * 测试用例 1: 测试初始化功能
     * 
     * 测试目标: 验证 DiskDataCenter.init() 方法是否正确初始化数据库
     * 
     * 预期结果:
     * - 数据库文件被创建
     * - 交换机表中应该有一个默认交换机 (名称为空字符串, 类型为 DIRECT)
     * - 队列表应该为空
     * - 绑定表应该为空
     */
    @Test
    public void testInit() {
        // 查询所有交换机、队列和绑定
        List<Exchange> exchangeList = diskDataCenter.selectAllExchanges();
        List<MSGQueue> queueList = diskDataCenter.selectAllQueues();
        List<Binding> bindingList = diskDataCenter.selectAllBindings();

        // 断言: 应该有且仅有一个默认交换机
        Assertions.assertEquals(1, exchangeList.size(), "初始化后应该有一个默认交换机");
        
        // 验证默认交换机的属性
        Exchange defaultExchange = exchangeList.get(0);
        Assertions.assertEquals("", defaultExchange.getName(), "默认交换机名称应该为空字符串");
        Assertions.assertEquals(ExchangeType.DIRECT, defaultExchange.getType(), "默认交换机类型应该为 DIRECT");
        Assertions.assertTrue(defaultExchange.isDurable(), "默认交换机应该是持久化的");
        Assertions.assertFalse(defaultExchange.isAutoDelete(), "默认交换机不应该自动删除");

        // 断言: 初始化后队列表和绑定表应该为空
        Assertions.assertEquals(0, queueList.size(), "初始化后队列表应该为空");
        Assertions.assertEquals(0, bindingList.size(), "初始化后绑定表应该为空");
    }

    /**
     * 辅助方法: 创建测试用的交换机对象
     */
    private Exchange createTestExchange(String exchangeName) {
        Exchange exchange = new Exchange();
        exchange.setName(exchangeName);
        exchange.setType(ExchangeType.TOPIC);
        exchange.setDurable(true);
        exchange.setAutoDelete(false);
        exchange.setArguments("key1", "value1");
        exchange.setArguments("key2", 100);
        return exchange;
    }

    /**
     * 测试用例 2: 测试插入交换机功能
     */
    @Test
    public void testInsertExchange() {
        Exchange exchange = createTestExchange("testExchange");
        diskDataCenter.insertExchange(exchange);
        
        List<Exchange> exchangeList = diskDataCenter.selectAllExchanges();
        Assertions.assertEquals(2, exchangeList.size(), "插入后应该有 2 个交换机");
        
        Exchange newExchange = exchangeList.get(1);
        Assertions.assertEquals("testExchange", newExchange.getName(), "交换机名称应该匹配");
        Assertions.assertEquals(ExchangeType.TOPIC, newExchange.getType(), "交换机类型应该为 TOPIC");
        Assertions.assertEquals("value1", newExchange.getArguments("key1"), "扩展属性应该匹配");
    }

    /**
     * 测试用例 3: 测试删除交换机功能
     */
    @Test
    public void testDeleteExchange() {
        Exchange exchange = createTestExchange("testExchange");
        diskDataCenter.insertExchange(exchange);
        
        List<Exchange> exchangeList = diskDataCenter.selectAllExchanges();
        Assertions.assertEquals(2, exchangeList.size());
        
        diskDataCenter.deleteExchange("testExchange");
        exchangeList = diskDataCenter.selectAllExchanges();
        Assertions.assertEquals(1, exchangeList.size(), "删除后应该只剩 1 个交换机");
    }

    /**
     * 辅助方法: 创建测试用的队列对象
     */
    private MSGQueue createTestQueue(String queueName) {
        MSGQueue queue = new MSGQueue();
        queue.setName(queueName);
        queue.setDurable(true);
        queue.setExclusive(false);
        queue.setAutoDelete(false);
        queue.setArguments("x", 100);
        return queue;
    }

    /**
     * 测试用例 4: 测试插入队列功能
     */
    @Test
    public void testInsertQueue() throws IOException {
        MSGQueue queue = createTestQueue("testQueue");
        diskDataCenter.insertQueue(queue);
        
        List<MSGQueue> queueList = diskDataCenter.selectAllQueues();
        Assertions.assertEquals(1, queueList.size(), "插入后应该有 1 个队列");
        
        MSGQueue newQueue = queueList.get(0);
        Assertions.assertEquals("testQueue", newQueue.getName(), "队列名称应该匹配");
        Assertions.assertEquals(100, newQueue.getArguments("x"), "扩展属性应该匹配");
    }

    /**
     * 测试用例 5: 测试删除队列功能
     */
    @Test
    public void testDeleteQueue() throws IOException {
        MSGQueue queue = createTestQueue("testQueue");
        diskDataCenter.insertQueue(queue);
        
        diskDataCenter.deleteQueue("testQueue");
        List<MSGQueue> queueList = diskDataCenter.selectAllQueues();
        Assertions.assertEquals(0, queueList.size(), "删除后队列表应该为空");
    }

    /**
     * 辅助方法: 创建测试用的绑定对象
     */
    private Binding createTestBinding(String exchangeName, String queueName) {
        Binding binding = new Binding();
        binding.setExchangeName(exchangeName);
        binding.setQueueName(queueName);
        binding.setBindingKey("testBindingKey");
        return binding;
    }

    /**
     * 测试用例 6: 测试插入绑定功能
     */
    @Test
    public void testInsertBinding() {
        Binding binding = createTestBinding("testExchange", "testQueue");
        diskDataCenter.insertBinding(binding);
        
        List<Binding> bindingList = diskDataCenter.selectAllBindings();
        Assertions.assertEquals(1, bindingList.size(), "插入后应该有 1 个绑定");
        
        Binding newBinding = bindingList.get(0);
        Assertions.assertEquals("testExchange", newBinding.getExchangeName(), "交换机名称应该匹配");
        Assertions.assertEquals("testQueue", newBinding.getQueueName(), "队列名称应该匹配");
    }

    /**
     * 测试用例 7: 测试删除绑定功能
     */
    @Test
    public void testDeleteBinding() {
        Binding binding = createTestBinding("testExchange", "testQueue");
        diskDataCenter.insertBinding(binding);
        
        Binding toDelete = createTestBinding("testExchange", "testQueue");
        diskDataCenter.deleteBinding(toDelete);
        
        List<Binding> bindingList = diskDataCenter.selectAllBindings();
        Assertions.assertEquals(0, bindingList.size(), "删除后绑定表应该为空");
    }

    /**
     * 辅助方法: 创建测试用的消息对象
     */
    private Message createTestMessage(String content) {
        return Message.createMessageWithId("testRoutingKey", null, content.getBytes());
    }

    /**
     * 测试用例 8: 测试发送消息功能
     */
    @Test
    public void testSendMessage() throws IOException, MqException, ClassNotFoundException {
        MSGQueue queue = createTestQueue("messageTestQueue");
        
        org.adam.mq.mqserver.datacenter.MessageFileManager messageFileManager = 
            new org.adam.mq.mqserver.datacenter.MessageFileManager();
        messageFileManager.createQueueFiles("messageTestQueue");
        
        org.springframework.test.util.ReflectionTestUtils.setField(diskDataCenter, "messageFileManager", messageFileManager);
        
        Message message = createTestMessage("测试消息内容");
        diskDataCenter.sendMessage(queue, message);
        
        LinkedList<Message> messages = diskDataCenter.loadAllMessageFromQueue("messageTestQueue");
        Assertions.assertEquals(1, messages.size(), "应该有 1 条消息");
        
        Message loadedMessage = messages.get(0);
        Assertions.assertEquals(message.getMessageId(), loadedMessage.getMessageId(), "消息 ID 应该匹配");
        Assertions.assertArrayEquals(message.getBody(), loadedMessage.getBody(), "消息内容应该匹配");
        
        messageFileManager.destroyQueueFiles("messageTestQueue");
    }

    /**
     * 测试用例 9: 测试删除消息功能
     */
    @Test
    public void testDeleteMessage() throws IOException, MqException, ClassNotFoundException {
        MSGQueue queue = createTestQueue("deleteTestQueue");
        
        org.adam.mq.mqserver.datacenter.MessageFileManager messageFileManager = 
            new org.adam.mq.mqserver.datacenter.MessageFileManager();
        messageFileManager.createQueueFiles("deleteTestQueue");
        
        org.springframework.test.util.ReflectionTestUtils.setField(diskDataCenter, "messageFileManager", messageFileManager);
        
        Message message1 = createTestMessage("消息1");
        Message message2 = createTestMessage("消息2");
        Message message3 = createTestMessage("消息3");
        
        diskDataCenter.sendMessage(queue, message1);
        diskDataCenter.sendMessage(queue, message2);
        diskDataCenter.sendMessage(queue, message3);
        
        diskDataCenter.deleteMessage(queue, message2);
        
        LinkedList<Message> messages = diskDataCenter.loadAllMessageFromQueue("deleteTestQueue");
        Assertions.assertEquals(2, messages.size(), "删除后应该剩余 2 条消息");
        
        Assertions.assertEquals(message1.getMessageId(), messages.get(0).getMessageId());
        Assertions.assertEquals(message3.getMessageId(), messages.get(1).getMessageId());
        
        messageFileManager.destroyQueueFiles("deleteTestQueue");
    }

    /**
     * 测试用例 10: 测试加载所有消息功能
     */
    @Test
    public void testLoadAllMessages() throws IOException, MqException, ClassNotFoundException {
        MSGQueue queue = createTestQueue("loadTestQueue");
        
        org.adam.mq.mqserver.datacenter.MessageFileManager messageFileManager = 
            new org.adam.mq.mqserver.datacenter.MessageFileManager();
        messageFileManager.createQueueFiles("loadTestQueue");
        
        org.springframework.test.util.ReflectionTestUtils.setField(diskDataCenter, "messageFileManager", messageFileManager);
        
        for (int i = 0; i < 10; i++) {
            Message message = createTestMessage("消息" + i);
            diskDataCenter.sendMessage(queue, message);
        }
        
        LinkedList<Message> messages = diskDataCenter.loadAllMessageFromQueue("loadTestQueue");
        Assertions.assertEquals(10, messages.size(), "应该加载 10 条消息");
        
        for (int i = 0; i < messages.size(); i++) {
            Assertions.assertArrayEquals(("消息" + i).getBytes(), messages.get(i).getBody());
        }
        
        messageFileManager.destroyQueueFiles("loadTestQueue");
    }
}