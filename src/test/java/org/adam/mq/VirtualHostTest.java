package org.adam.mq;

import org.adam.mq.common.Consumer;
import org.adam.mq.mqserver.VirtualHost;
import org.adam.mq.mqserver.core.BasicProperties;
import org.adam.mq.mqserver.core.ExchangeType;
import org.apache.tomcat.util.http.fileupload.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.File;
import java.io.IOException;

@SpringBootTest
public class VirtualHostTest {
    private VirtualHost virtualHost = null;

    @BeforeEach
    public void setup() {
        MqApplication.context = SpringApplication.run(MqApplication.class); // 启动SpringBoot应用程序
        virtualHost = new VirtualHost("test_vhost");
    }

    @AfterEach
    public void teardown() throws IOException {
        MqApplication.context.close();
        virtualHost = null;
        // 把硬盘的目录删除掉
        File dataDir = new File("./data");
        FileUtils.deleteDirectory(dataDir);
    }

    @Test
    public void testExchangeDeclare() {
        boolean success = virtualHost.exchangeDeclare("test_Exchange", ExchangeType.DIRECT, true, false, null);
        Assertions.assertTrue(success);
    }

    @Test
    public void testExchangeDelete() {
        boolean success = virtualHost.exchangeDeclare("test_Exchange", ExchangeType.DIRECT, true, false, null);
        Assertions.assertTrue(success);

        boolean success_delete = virtualHost.exchangeDelete("test_Exchange");
        Assertions.assertTrue(success_delete);
    }

    @Test
    public void testQueueDeclare() {
        boolean success = virtualHost.queueDeclare("test_Queue", true, false, false, null);
        Assertions.assertTrue(success);
    }

    @Test
    public void testQueueDelete() {
        boolean success = virtualHost.queueDeclare("test_Queue", true, false, false, null);
        Assertions.assertTrue(success);

        success = virtualHost.queueDelete("test_Queue");
        Assertions.assertTrue(success);
    }

    @Test
    public void testQueueBind() {
        boolean queueDeclare = virtualHost.queueDeclare("test_Queue", true, false, false, null);
        Assertions.assertTrue(queueDeclare);

        boolean exchangeDeclare = virtualHost.exchangeDeclare("test_Exchange", ExchangeType.DIRECT, true, false, null);
        Assertions.assertTrue(exchangeDeclare);

        boolean queueBind = virtualHost.queueBind("test_Queue", "test_Exchange", "test_BindingKey");
        Assertions.assertTrue(queueBind);
    }

    @Test
    public void testQueueUnBind() {
        boolean queueDeclare = virtualHost.queueDeclare("test_Queue", true, false, false, null);
        Assertions.assertTrue(queueDeclare);

        boolean exchangeDeclare = virtualHost.exchangeDeclare("test_Exchange", ExchangeType.DIRECT, true, false, null);
        Assertions.assertTrue(exchangeDeclare);

        boolean queueBind = virtualHost.queueBind("test_Queue", "test_Exchange", "test_BindingKey");
        Assertions.assertTrue(queueBind);

        boolean queueUnBind = virtualHost.queueUnbind("test_Queue", "test_Exchange");
        Assertions.assertTrue(queueUnBind);
    }

    @Test
    public void testBasicPublish() {
        boolean exchangeDeclare = virtualHost.exchangeDeclare("test_Exchange", ExchangeType.DIRECT, true, false, null);
        Assertions.assertTrue(exchangeDeclare);

        boolean queueDeclare = virtualHost.queueDeclare("test_Queue", true, false, false, null);
        Assertions.assertTrue(queueDeclare);

        boolean queueBind = virtualHost.queueBind("test_Queue", "test_Exchange", "test_BindingKey");
        Assertions.assertTrue(queueBind);

        String message = "Hello, World!";
        boolean publishSuccess = virtualHost.basicPublish("test_Exchange", "test_BindingKey", null, message.getBytes());
        Assertions.assertTrue(publishSuccess);
    }

    @Test
    // 先订阅队列，后发送消息
    public void testBasicConsume1() {
        boolean exchangeDeclare = virtualHost.exchangeDeclare("test_Exchange", ExchangeType.DIRECT, true, false, null);
        Assertions.assertTrue(exchangeDeclare);

        boolean queueDeclare = virtualHost.queueDeclare("test_Queue", true, false, false, null);
        Assertions.assertTrue(queueDeclare);

        // 订阅队列
        virtualHost.basicConsume("testConsumerTag", "test_Queue", true, new Consumer() {
            @Override
            public void handleDelivery(String consumerTag, BasicProperties properties, byte[] body) {
                // 消费者自身设定的回调方法
                System.out.println("[Consumer] messageId: " + properties.getMessageId());
                System.out.println("[Consumer] body: " + new String(body, 0 , body.length));
            }
        });
    }
    @Test
    // 先发送消息，再订阅队列
    public void testBasicConsume2() {

    }
}
