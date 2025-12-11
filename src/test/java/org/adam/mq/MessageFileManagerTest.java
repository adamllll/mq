package org.adam.mq;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.adam.mq.common.MqException;
import org.adam.mq.mqserver.core.MSGQueue;
import org.adam.mq.mqserver.core.Message;
import org.adam.mq.mqserver.datacenter.MessageFileManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.util.ReflectionTestUtils;

@SpringBootTest
public class MessageFileManagerTest {
    private MessageFileManager messageFileManager = new MessageFileManager();

    private static final String QUEUE_NAME_1 = "test_queue_1";
    private static final String QUEUE_NAME_2 = "test_queue_2";

    // 这个方法是每个用例执行之前的准备工作
    @BeforeEach
    public void setUp() throws IOException {
        // 准备阶段，创建出两个队列，以备后用
        messageFileManager.createQueueFiles(QUEUE_NAME_1);
        messageFileManager.createQueueFiles(QUEUE_NAME_2);
    }

    // 这个方法是每个用例执行之后的清理工作
    @AfterEach
    public void tearDown() throws IOException {
        // 清理阶段，删除掉刚才创建的两个队列
        messageFileManager.destroyQueueFiles(QUEUE_NAME_1);
        messageFileManager.destroyQueueFiles(QUEUE_NAME_2);
    }

    @Test
    public void testCreateFiles() {
        // 测试创建文件的功能是否正常，创建队列文件已经在上面setUp方法中完成，这里只需要验证文件是否存在即可
        File queueDataFile1 = new File("./data/" + QUEUE_NAME_1 + "/queue_data.txt");
        Assertions.assertEquals(true, queueDataFile1.isFile());
        File queueStateFile1 = new File("./data/" + QUEUE_NAME_1 + "/queue_stat.txt");
        Assertions.assertEquals(true, queueStateFile1.isFile());

        File queueDataFile2 = new File("./data/" + QUEUE_NAME_2 + "/queue_data.txt");
        Assertions.assertEquals(true, queueDataFile2.isFile());
        File queueStateFile2 = new File("./data/" + QUEUE_NAME_2 + "/queue_stat.txt");
        Assertions.assertEquals(true, queueStateFile2.isFile());
    }

    @Test
    public void testReadWriteStat() {
        // 测试读写队列状态的功能是否正常
        MessageFileManager.Stat stat = new MessageFileManager.Stat();
        stat.totalCount = 100; // 100条消息
        stat.validCount = 50; // 50条有效消息

        // 此处就需要使用反射的方式，来调用writeStat方法和readStat方法
        // Java原生的反射API使用起来比较麻烦，此处使用Spring封装好的反射工具类,ReflectionTestUtils
        ReflectionTestUtils.invokeMethod(messageFileManager, "writeStat", QUEUE_NAME_1, stat);

        // 写入完毕之后，再读取出来，验证是否和写入前的数据一致
        MessageFileManager.Stat newStat = ReflectionTestUtils.invokeMethod(messageFileManager, "readStat", QUEUE_NAME_1);
        Assertions.assertEquals(100, newStat.totalCount);
        Assertions.assertEquals(50, newStat.validCount);
    }
    
    // 构造一个测试用的队列对象
    private MSGQueue createTestQueue(String queueName) {;
        MSGQueue queue = new MSGQueue();
        queue.setName(queueName);
        queue.setDurable(true);
        queue.setAutoDelete(false);
        queue.setExclusive(false);
        return queue;
    }
    // 构造一个测试用的消息对象
    private Message createTestMessage(String content) {
        Message message = Message.createMessageWithId("testRoutingKey",null, content.getBytes());
        return message;        
    }
    
    @Test
    public void testSendMessage() throws IOException, MqException, ClassNotFoundException {
        // 构造出消息，并且构造出队列
        Message message = createTestMessage("testMessage");
        // 此处创建的queue对象的name不能随便写，因为MessageFileManager会根据队列名称来定位对应的文件
        // 需要保证这个队列对象对应的目录和文件都存在
        MSGQueue queue = createTestQueue(QUEUE_NAME_1); // 使用上面创建的测试用队列对象
        
        // 调用sendMessage方法，将消息写入到文件中
        messageFileManager.sendMessage(queue, message);
        
        // 发送完毕之后，再从文件中读取出来，验证内容是否正确
        // 检查统计文件
        MessageFileManager.Stat stat = ReflectionTestUtils.invokeMethod(messageFileManager, "readStat", QUEUE_NAME_1);
        Assertions.assertEquals(1, stat.totalCount);
        Assertions.assertEquals(1, stat.validCount);
        
        // 检查数据文件data
        LinkedList<Message> messages = messageFileManager.loadAllMessageFromQueue(QUEUE_NAME_1);
        Assertions.assertEquals(1, messages.size()); // 应该只有一条消息
        Message curMessage = messages.get(0); // 取出第一条消息
        Assertions.assertEquals(message.getMessageId(), curMessage.getMessageId()); // 验证消息ID是否一致
        Assertions.assertEquals(message.getRoutingKey(), curMessage.getRoutingKey()); // 验证路由键是否一致
        Assertions.assertEquals(message.getDeliveryMode(), curMessage.getDeliveryMode());
        // 比较两个字节数组的内容是否一致，不能直接使用assertEquals方法
        Assertions.assertArrayEquals(message.getBody(), curMessage.getBody());
        System.out.println("message:" + curMessage);
    }

    @Test
    public void testLoadAllMessageFromQueue() throws IOException, MqException, ClassNotFoundException {
        // 往队列中插入100条消息，然后再全部读取出来，验证数量和内容是否正确
        MSGQueue queue = createTestQueue(QUEUE_NAME_1);
        List<Message> expectedMessages = new LinkedList<>();
        for (int i = 0; i < 100; i++) {
            Message message = createTestMessage("testMessage" + i);
            messageFileManager.sendMessage(queue, message);
            expectedMessages.add(message);
        }
        //读取所有消息
        LinkedList<Message> actualmessage = messageFileManager.loadAllMessageFromQueue(QUEUE_NAME_1);
        // 验证数量是否正确
        Assertions.assertEquals(expectedMessages.size(), actualmessage.size());
        for (int i = 0; i < expectedMessages.size(); i++) {
            Message expectedMessage = expectedMessages.get(i);
            Message actualMessage = actualmessage.get(i);
            System.out.println("[" + i + "] " + "actualMessage =" + actualMessage);

            Assertions.assertEquals(expectedMessage.getMessageId(), actualMessage.getMessageId());
            Assertions.assertEquals(expectedMessage.getRoutingKey(), actualMessage.getRoutingKey());
            Assertions.assertEquals(expectedMessage.getDeliveryMode(), actualMessage.getDeliveryMode());
            Assertions.assertArrayEquals(expectedMessage.getBody(), actualMessage.getBody());
            Assertions.assertEquals(0x1, actualMessage.getIsValid());
        }
    }

    @Test
    public void testDeleteMessage() throws IOException, MqException, ClassNotFoundException {
        // 创建队列，写入10个消息，删除其中几个消息，再把所有消息读取出来，判定是否符合预期
        MSGQueue queue = createTestQueue(QUEUE_NAME_1);
        List<Message> expectedMessages = new LinkedList<>();
        for (int i = 0; i < 10; i++) {
            Message message = createTestMessage("testMessage" + i);
            messageFileManager.sendMessage(queue,message);
            expectedMessages.add(message);
        }

        // 删除其中的三个消息
        messageFileManager.deleteMessage(queue, expectedMessages.get(7));
        messageFileManager.deleteMessage(queue, expectedMessages.get(8));
        messageFileManager.deleteMessage(queue, expectedMessages.get(9));

        // 对比内容是否符合要求
        LinkedList<Message> actualMessages = messageFileManager.loadAllMessageFromQueue(QUEUE_NAME_1);
        Assertions.assertEquals(7, actualMessages.size());
        for (int i = 0; i < actualMessages.size(); i++) {
            Message expectedMessage = expectedMessages.get(i);
            Message actualMessage = actualMessages.get(i);
            System.out.println("[" + i + "] " + "actualMessage =" + actualMessage);

            Assertions.assertEquals(expectedMessage.getMessageId(), actualMessage.getMessageId());
            Assertions.assertEquals(expectedMessage.getRoutingKey(), actualMessage.getRoutingKey());
            Assertions.assertEquals(expectedMessage.getDeliveryMode(), actualMessage.getDeliveryMode());
            Assertions.assertArrayEquals(expectedMessage.getBody(), actualMessage.getBody());
        }
    }

    // 测试垃圾回收
    @Test
    public void testGC() throws IOException, MqException, ClassNotFoundException {
        // 先往队列中写入100条消息，获取文件大小
        // 再把100个消息中的一半全部删除(下标为偶数的消息全部删除)
        // 再手动调用gc方法，检测得到的文件大小是否缩小了

        MSGQueue queue = createTestQueue(QUEUE_NAME_1);
        List<Message> expectedMessages = new LinkedList<>();
        for (int i = 0; i < 100; i++) {
            Message message = createTestMessage("testMessage" + i);
            messageFileManager.sendMessage(queue, message);
            expectedMessages.add(message);
        }
        // 获取gc前文件大小
        File beforeGCFile = new File("./data/" + QUEUE_NAME_1 + "/queue_data.txt");
        long beforeGCLength = beforeGCFile.length();
        // 删除偶数下标的消息
        for (int i = 0; i < 100; i += 2) {
            messageFileManager.deleteMessage(queue, expectedMessages.get(i));
        }
        // 手动调用gc方法
        messageFileManager.gc(queue);
        // 重新读取文件，验证新的文件内柔是否和之前的内容匹配
        List<Message> actualMessages = messageFileManager.loadAllMessageFromQueue(QUEUE_NAME_1);
        Assertions.assertEquals(50, actualMessages.size());
        for (int i = 0; i < actualMessages.size(); i++) {
            // 之前把偶数下标的消息删除了，所以现在实际的消息列表中，应该是下标为奇数的那些消息
            // actual 的第0条消息，应该对应 expected 的第1条消息
            // actual 的第1条消息，应该对应 expected 的第3条消息
            // 以此类推 actual 的第i条消息，应该对应 expected 的第i*2+1条消息
            Message expectedMessage = expectedMessages.get(i * 2 + 1);
            Message actualMessage = actualMessages.get(i);

            Assertions.assertEquals(expectedMessage.getMessageId(), actualMessage.getMessageId());
            Assertions.assertEquals(expectedMessage.getRoutingKey(), actualMessage.getRoutingKey());
            Assertions.assertEquals(expectedMessage.getDeliveryMode(), actualMessage.getDeliveryMode());
            Assertions.assertArrayEquals(expectedMessage.getBody(), actualMessage.getBody());
        }
        // 获取gc后文件大小
        File afterGCFile = new File("./data/" + QUEUE_NAME_1 + "/queue_data.txt");
        long afterGCLength = afterGCFile.length();
        System.out.println("beforeGCLength=" + beforeGCLength);
        System.out.println("afterGCLength=" + afterGCLength);
        Assertions.assertTrue(afterGCLength < beforeGCLength);
    }
}
