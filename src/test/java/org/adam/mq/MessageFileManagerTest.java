package org.adam.mq;

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

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

@SpringBootTest
public class MessageFileManagerTest {
    private MessageFileManager messageFileManager = new MessageFileManager();

    private static final String QUEUE_NAME_1 = "test_queue_1";
    private static final String QUEUE_NAME_2 = "test_queue_2";

    @BeforeEach
    public void setUp() throws IOException {
        messageFileManager.createQueueFiles(QUEUE_NAME_1);
        messageFileManager.createQueueFiles(QUEUE_NAME_2);
    }

    @AfterEach
    public void tearDown() throws IOException {
        messageFileManager.destroyQueueFiles(QUEUE_NAME_1);
        messageFileManager.destroyQueueFiles(QUEUE_NAME_2);
    }

    @Test
    public void testCreateFiles() {
        File queueDataFile1 = new File("./data/" + QUEUE_NAME_1 + "/queue_data.txt");
        Assertions.assertTrue(queueDataFile1.isFile());
        File queueStateFile1 = new File("./data/" + QUEUE_NAME_1 + "/queue_stat.txt");
        Assertions.assertTrue(queueStateFile1.isFile());

        File queueDataFile2 = new File("./data/" + QUEUE_NAME_2 + "/queue_data.txt");
        Assertions.assertTrue(queueDataFile2.isFile());
        File queueStateFile2 = new File("./data/" + QUEUE_NAME_2 + "/queue_stat.txt");
        Assertions.assertTrue(queueStateFile2.isFile());
    }

    @Test
    public void testReadWriteStat() {
        MessageFileManager.Stat stat = new MessageFileManager.Stat();
        stat.totalCount = 100;
        stat.validCount = 50;

        ReflectionTestUtils.invokeMethod(messageFileManager, "writeStat", QUEUE_NAME_1, stat);

        MessageFileManager.Stat newStat = ReflectionTestUtils.invokeMethod(messageFileManager, "readStat", QUEUE_NAME_1);
        Assertions.assertEquals(100, newStat.totalCount);
        Assertions.assertEquals(50, newStat.validCount);
    }

    private MSGQueue createTestQueue(String queueName) {
        MSGQueue queue = new MSGQueue();
        queue.setName(queueName);
        queue.setDurable(true);
        queue.setAutoDelete(false);
        queue.setExclusive(false);
        return queue;
    }

    private Message createTestMessage(String content) {
        return Message.createMessageWithId("testRoutingKey", null, content.getBytes());
    }

    @Test
    public void testSendMessage() throws IOException, MqException, ClassNotFoundException {
        Message message = createTestMessage("testMessage");
        MSGQueue queue = createTestQueue(QUEUE_NAME_1);

        messageFileManager.sendMessage(queue, message);

        MessageFileManager.Stat stat = ReflectionTestUtils.invokeMethod(messageFileManager, "readStat", QUEUE_NAME_1);
        Assertions.assertEquals(1, stat.totalCount);
        Assertions.assertEquals(1, stat.validCount);

        LinkedList<Message> messages = messageFileManager.loadAllMessageFromQueue(QUEUE_NAME_1);
        Assertions.assertEquals(1, messages.size());
        Message currentMessage = messages.get(0);
        Assertions.assertEquals(message.getMessageId(), currentMessage.getMessageId());
        Assertions.assertEquals(message.getRoutingKey(), currentMessage.getRoutingKey());
        Assertions.assertEquals(message.getDeliveryMode(), currentMessage.getDeliveryMode());
        Assertions.assertArrayEquals(message.getBody(), currentMessage.getBody());
    }

    @Test
    public void testLoadAllMessageFromQueue() throws IOException, MqException, ClassNotFoundException {
        MSGQueue queue = createTestQueue(QUEUE_NAME_1);
        List<Message> expectedMessages = new LinkedList<>();
        for (int i = 0; i < 100; i++) {
            Message message = createTestMessage("testMessage" + i);
            messageFileManager.sendMessage(queue, message);
            expectedMessages.add(message);
        }

        LinkedList<Message> actualMessages = messageFileManager.loadAllMessageFromQueue(QUEUE_NAME_1);
        Assertions.assertEquals(expectedMessages.size(), actualMessages.size());
        for (int i = 0; i < expectedMessages.size(); i++) {
            Message expectedMessage = expectedMessages.get(i);
            Message actualMessage = actualMessages.get(i);

            Assertions.assertEquals(expectedMessage.getMessageId(), actualMessage.getMessageId());
            Assertions.assertEquals(expectedMessage.getRoutingKey(), actualMessage.getRoutingKey());
            Assertions.assertEquals(expectedMessage.getDeliveryMode(), actualMessage.getDeliveryMode());
            Assertions.assertArrayEquals(expectedMessage.getBody(), actualMessage.getBody());
            Assertions.assertEquals(0x1, actualMessage.getIsValid());
        }
    }

    @Test
    public void testDeleteMessage() throws IOException, MqException, ClassNotFoundException {
        MSGQueue queue = createTestQueue(QUEUE_NAME_1);
        List<Message> expectedMessages = new LinkedList<>();
        for (int i = 0; i < 10; i++) {
            Message message = createTestMessage("testMessage" + i);
            messageFileManager.sendMessage(queue, message);
            expectedMessages.add(message);
        }

        messageFileManager.deleteMessage(queue, expectedMessages.get(7));
        messageFileManager.deleteMessage(queue, expectedMessages.get(8));
        messageFileManager.deleteMessage(queue, expectedMessages.get(9));

        LinkedList<Message> actualMessages = messageFileManager.loadAllMessageFromQueue(QUEUE_NAME_1);
        Assertions.assertEquals(7, actualMessages.size());
        for (int i = 0; i < actualMessages.size(); i++) {
            Message expectedMessage = expectedMessages.get(i);
            Message actualMessage = actualMessages.get(i);

            Assertions.assertEquals(expectedMessage.getMessageId(), actualMessage.getMessageId());
            Assertions.assertEquals(expectedMessage.getRoutingKey(), actualMessage.getRoutingKey());
            Assertions.assertEquals(expectedMessage.getDeliveryMode(), actualMessage.getDeliveryMode());
            Assertions.assertArrayEquals(expectedMessage.getBody(), actualMessage.getBody());
        }
    }

    @Test
    public void testGC() throws IOException, MqException, ClassNotFoundException {
        MSGQueue queue = createTestQueue(QUEUE_NAME_1);
        List<Message> expectedMessages = new LinkedList<>();
        for (int i = 0; i < 100; i++) {
            Message message = createTestMessage("testMessage" + i);
            messageFileManager.sendMessage(queue, message);
            expectedMessages.add(message);
        }

        File beforeGCFile = new File("./data/" + QUEUE_NAME_1 + "/queue_data.txt");
        long beforeGCLength = beforeGCFile.length();

        for (int i = 0; i < 100; i += 2) {
            messageFileManager.deleteMessage(queue, expectedMessages.get(i));
        }

        messageFileManager.gc(queue);

        List<Message> actualMessages = messageFileManager.loadAllMessageFromQueue(QUEUE_NAME_1);
        Assertions.assertEquals(50, actualMessages.size());
        for (int i = 0; i < actualMessages.size(); i++) {
            Message expectedMessage = expectedMessages.get(i * 2 + 1);
            Message actualMessage = actualMessages.get(i);

            Assertions.assertEquals(expectedMessage.getMessageId(), actualMessage.getMessageId());
            Assertions.assertEquals(expectedMessage.getRoutingKey(), actualMessage.getRoutingKey());
            Assertions.assertEquals(expectedMessage.getDeliveryMode(), actualMessage.getDeliveryMode());
            Assertions.assertArrayEquals(expectedMessage.getBody(), actualMessage.getBody());
        }

        File afterGCFile = new File("./data/" + QUEUE_NAME_1 + "/queue_data.txt");
        long afterGCLength = afterGCFile.length();
        Assertions.assertTrue(afterGCLength < beforeGCLength);
    }

    @Test
    public void testCheckGCThresholdBoundary() {
        MessageFileManager.Stat stat = new MessageFileManager.Stat();
        stat.totalCount = 2000;
        stat.validCount = 999;
        ReflectionTestUtils.invokeMethod(messageFileManager, "writeStat", QUEUE_NAME_1, stat);
        Assertions.assertFalse(messageFileManager.checkGC(QUEUE_NAME_1));

        stat.totalCount = 2001;
        stat.validCount = 999;
        ReflectionTestUtils.invokeMethod(messageFileManager, "writeStat", QUEUE_NAME_1, stat);
        Assertions.assertTrue(messageFileManager.checkGC(QUEUE_NAME_1));
    }

    @Test
    public void testCheckGCDoesNotTriggerAtExactlyHalfValidRatio() {
        MessageFileManager.Stat stat = new MessageFileManager.Stat();
        stat.totalCount = 3000;
        stat.validCount = 1500;
        ReflectionTestUtils.invokeMethod(messageFileManager, "writeStat", QUEUE_NAME_1, stat);
        Assertions.assertFalse(messageFileManager.checkGC(QUEUE_NAME_1));
    }
}
