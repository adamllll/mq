package org.adam.mq.demo;

import org.adam.mq.mqclient.Channel;
import org.adam.mq.mqclient.Connection;
import org.adam.mq.mqclient.ConnectionFactory;
import org.adam.mq.mqserver.core.ExchangeType;

import java.io.IOException;

/**
 * 表示一个生产者
 * 通常这是一个单独的服务器程序
 */
public class DemoProducer {
    public static void main(String[] args) throws IOException, InterruptedException {
        System.out.println("启动生产者");
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("127.0.0.1");
        connectionFactory.setPort(9090);

        Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();

        // 创建交换机队列
        channel.exchangeDeclare("demo_exchange", ExchangeType.DIRECT, true, false, null);
        channel.queueDeclare("demo_queue", true, false, false, null);
        channel.queueBind("demo_queue", "demo_exchange", "demo_bindingkey");

        // 创建一个消息并发送
        byte[] messageBody = "Hello, World!".getBytes();
        boolean success = channel.basicPublish("demo_exchange", "demo_bindingkey", null, messageBody);
        System.out.println("消息已发送 success=" + success);

        Thread.sleep(500);
        // 清理资源
        channel.queueUnbind("demo_queue", "demo_exchange");
        channel.queueDelete("demo_queue");
        channel.exchangeDelete("demo_exchange");
        channel.closeChannel();
        connection.close();
        System.out.println("生产者已关闭");
    }
}
