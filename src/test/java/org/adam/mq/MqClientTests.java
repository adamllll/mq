package org.adam.mq;

import org.adam.mq.common.Consumer;
import org.adam.mq.common.MqException;
import org.adam.mq.mqclient.Channel;
import org.adam.mq.mqclient.Connection;
import org.adam.mq.mqclient.ConnectionFactory;
import org.adam.mq.mqserver.BrokerServer;
import org.adam.mq.mqserver.core.BasicProperties;
import org.adam.mq.mqserver.core.ExchangeType;
import org.apache.tomcat.util.http.fileupload.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;

import java.io.File;
import java.io.IOException;

public class MqClientTests {
    private BrokerServer brokerServer = null;
    private ConnectionFactory connectionFactory = null;
    private Thread serverThread = null;

    @BeforeEach
    public void setUp() throws IOException {
        // 1. 启动服务器
        MqApplication.context = SpringApplication.run(MqApplication.class);
        brokerServer = new BrokerServer(9090);

        // start 会进入阻塞，所以放在单独的线程中启动
        serverThread = new Thread(() -> {
            try {
                brokerServer.start();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        serverThread.start();

        // 2，配置 ConnectionFactory
        connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("127.0.0.1");
        connectionFactory.setPort(9090);
    }
    @AfterEach
    public void tearDown() throws IOException {
        // 停止服务器
        if (brokerServer != null) {
            brokerServer.stop();
        }
        if (serverThread != null && serverThread.isAlive()) {
            serverThread.interrupt();
        }
        // 关闭 Spring 上下文
        MqApplication.context.close();

        // 删除必要的文件
        File file = new File("./data");
        FileUtils.deleteDirectory(file);

        connectionFactory = null;
    }

    // 测试 Connection 相关功能
    @Test
    public void testConnection() throws IOException {
        Connection connection = connectionFactory.newConnection();
        Assertions.assertNotNull(connection);
    }

    // 测试 Channel 相关功能
    @Test
    public void testChannel() throws IOException {
      Connection connection = connectionFactory.newConnection();
      Assertions.assertNotNull(connection);
      Channel channel = connection.createChannel();
      Assertions.assertNotNull(channel);
    }

    // 测试 Exchange 相关功能
    @Test
    public void testExchange() throws IOException {
        Connection connection = connectionFactory.newConnection();
        Assertions.assertNotNull(connection);

        Channel channel = connection.createChannel();
        Assertions.assertNotNull(channel);

        boolean success =  channel.exchangeDeclare("test_exchange", ExchangeType.DIRECT, true, false, null);
        Assertions.assertTrue(success);

        success = channel.exchangeDelete("test_exchange");
        Assertions.assertTrue(success);

        // 稳妥起见，关闭资源
        channel.closeChannel();
        connection.close();
    }
    // 测试 Queue
    @Test
    public void testQueue() throws IOException {
        Connection connection = connectionFactory.newConnection();
        Assertions.assertNotNull(connection);

        Channel channel = connection.createChannel();
        Assertions.assertNotNull(channel);

        boolean success = channel.queueDeclare("test_queue", true, false, false, null);
        Assertions.assertTrue(success);

        success = channel.queueDelete("test_queue");
        Assertions.assertTrue(success);

        // 稳妥起见，关闭资源
        channel.closeChannel();
        connection.close();
    }

    // 测试 Binding
    @Test
    public void testBinding() throws IOException{
        Connection connection = connectionFactory.newConnection();
        Assertions.assertNotNull(connection);

        Channel channel = connection.createChannel();
        Assertions.assertNotNull(channel);

        boolean success =  channel.exchangeDeclare("test_exchange", ExchangeType.DIRECT, true, false, null);
        Assertions.assertTrue(success);

        success = channel.queueDeclare("test_queue", true, false, false, null);
        Assertions.assertTrue(success);

        success = channel.queueBind("test_queue", "test_exchange", "test_bindingkey");
        Assertions.assertTrue(success);

        success = channel.queueUnbind("test_queue", "test_exchange");
        Assertions.assertTrue(success);

        success = channel.queueDelete("test_queue");
        Assertions.assertTrue(success);

        success = channel.exchangeDelete("test_exchange");
        Assertions.assertTrue(success);

        // 稳妥起见，关闭资源
        channel.closeChannel();
        connection.close();
    }

    // 测试发送消息
    @Test
    public void testMessage() throws IOException, MqException, InterruptedException {
        Connection connection = connectionFactory.newConnection();
        Assertions.assertNotNull(connection);
        Channel channel = connection.createChannel();
        Assertions.assertNotNull(channel);

        boolean success =  channel.exchangeDeclare("test_exchange", ExchangeType.DIRECT, true, false, null);
        Assertions.assertTrue(success);

        success = channel.queueDeclare("test_queue", true, false, false, null);
        Assertions.assertTrue(success);

        success = channel.queueBind("test_queue", "test_exchange", "test_queue");
        Assertions.assertTrue(success);

        byte[] requestBody = "Hello Mq Server".getBytes();
            success = channel.basicPublish("test_exchange", "test_queue", null, requestBody);
            Assertions.assertTrue(success);

            success = channel.basicConsume("test_queue", true, new Consumer() {
                @Override
                public void handleDelivery(String consumerTag, BasicProperties properties, byte[] body) throws MqException, IOException {
                    System.out.println("[消费数据]: 开始");
                    System.out.println("消费者标签: " + consumerTag);
                    System.out.println("消息属性: " + properties);
                    Assertions.assertArrayEquals(requestBody, body);
                    System.out.println("[消费数据]: 结束");

                }
            });
            Assertions.assertTrue(success);
            // 等待一会儿，确保消息被消费
            Thread.sleep(500);

            // 清理资源
            channel.queueUnbind("test_queue", "test_exchange");
            channel.queueDelete("test_queue");
            channel.exchangeDelete("test_exchange");

            // 稳妥起见，关闭资源
            channel.closeChannel();
            connection.close();

    }
}
