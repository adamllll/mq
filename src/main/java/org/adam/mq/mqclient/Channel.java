package org.adam.mq.mqclient;

import org.adam.mq.common.*;
import org.adam.mq.mqserver.core.BasicProperties;
import org.adam.mq.mqserver.core.ExchangeType;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class Channel {
    // 每个 Channel 都有一个唯一标识
    private String channelId;
    private Connection connection;
    // 用来存储后续客户端收到的服务器的响应
    private ConcurrentHashMap<String, BasicReturns> basicReturnsMap = new ConcurrentHashMap<>();
    // 如果当前 Channel 订阅了某个队列，就需要记录下回调函数，当收到消息时调用这个回调函数
    // 一个 Channel 只能有一个 Consumer
    private Consumer consumer = null;

    public Channel(String channelId, Connection connection) {
        this.channelId = channelId;
        this.connection = connection;
    }
    // 在这个方法中和服务器进行交互，告诉服务器创建一个新的 Channel
    public boolean createChannel() throws IOException {
        // 对于创建 Channel操作来说此处 payload就是一个 basicArguments对象
        BasicArguments basicArguments = new BasicArguments();
        basicArguments.setChannelId(channelId);
        basicArguments.setRid(generateRid());
        byte[] payload = BinaryTool.toBytes(basicArguments);
        // 构造 Request 对象
        Request request = new Request();
        request.setType(0x1); // 创建 Channel 请求
        request.setLength(payload.length);
        request.setPayload(payload);

        // 发送请求
        connection.wirteRequest(request);
        // 等待服务器响应
        BasicReturns basicReturns = waitResult(basicArguments.getRid());
        return basicReturns.isSuccess();
    }

    // 使用这个方法阻塞等待服务器的响应
    private BasicReturns waitResult(String rid) {
        BasicReturns basicReturns = null;
        while ((basicReturns = basicReturnsMap.get(rid)) == null) {
            // 什么都不做，阻塞等待
            synchronized (this) {
                try {
                    wait();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        // 读取成功之后，从 map 中移除这个响应
        basicReturnsMap.remove(rid);
        return basicReturns;
    }

    private String generateRid() {
        return "R" + UUID.randomUUID().toString();
    }

    // 关闭 Channel
    public boolean closeChannel() throws IOException {
        BasicArguments basicArguments = new BasicArguments();
        basicArguments.setChannelId(channelId);
        basicArguments.setRid(generateRid());
        byte[] payload = BinaryTool.toBytes(basicArguments);
        // 构造 Request 对象
        Request request = new Request();
        request.setType(0x2); // 关闭 Channel 请求
        request.setLength(payload.length);
        request.setPayload(payload);

        try {
            // 发送请求
            connection.wirteRequest(request);
            // 等待服务器响应
            BasicReturns basicReturns = waitResult(basicArguments.getRid());
            return basicReturns.isSuccess();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // 创建交换机
    public boolean exchangeDeclare(String exchangeName, ExchangeType exchangeType, boolean durable, boolean autoDelete,
                                Map<String, Object> arguments) throws IOException {
        ExchangeDeclareArguments exchangeDeclareArguments = new ExchangeDeclareArguments();
        exchangeDeclareArguments.setChannelId(channelId);
        exchangeDeclareArguments.setRid(generateRid());
        exchangeDeclareArguments.setExchangeName(exchangeName);
        exchangeDeclareArguments.setExchangeType(exchangeType);
        exchangeDeclareArguments.setDurable(durable);
        exchangeDeclareArguments.setAutoDelete(autoDelete);
        exchangeDeclareArguments.setArguments(arguments);
        byte[] payload = BinaryTool.toBytes(exchangeDeclareArguments);

        // 构造 Request 对象
        Request request = new Request();
        request.setType(0x3); // 创建交换机请求
        request.setLength(payload.length);
        request.setPayload(payload);

        // 发送请求
        connection.wirteRequest(request);
        // 等待服务器响应
        BasicReturns basicReturns = waitResult(exchangeDeclareArguments.getRid());
        return basicReturns.isSuccess();
    }

    // 删除交换机
    public boolean exchangeDelete(String exchangeName) throws IOException {
        ExchangeDeleteArguments exchangeDeleteArguments = new ExchangeDeleteArguments();
        exchangeDeleteArguments.setChannelId(channelId);
        exchangeDeleteArguments.setRid(generateRid());
        exchangeDeleteArguments.setExchangeName(exchangeName);
        byte[] payload = BinaryTool.toBytes(exchangeDeleteArguments);

        // 构造 Request 对象
        Request request = new Request();
        request.setType(0x4); // 删除交换机请求
        request.setLength(payload.length);
        request.setPayload(payload);

        // 发送请求
        connection.wirteRequest(request);
        // 等待服务器响应
        BasicReturns basicReturns = waitResult(exchangeDeleteArguments.getRid());
        return basicReturns.isSuccess();
    }

    //  创建队列
    public boolean queueDeclare(String queueName, boolean durable, boolean exclusive, boolean autoDelete,
                            Map<String, Object> arguments) throws IOException {
        QueueDeclareArguments queueDeclareArguments = new QueueDeclareArguments();
        queueDeclareArguments.setChannelId(channelId);
        queueDeclareArguments.setRid(generateRid());
        queueDeclareArguments.setQueueName(queueName);
        queueDeclareArguments.setDurable(durable);
        queueDeclareArguments.setExclusive(exclusive);
        queueDeclareArguments.setAutoDelete(autoDelete);
        queueDeclareArguments.setArguments(arguments);
        byte[] payload = BinaryTool.toBytes(queueDeclareArguments);

        // 构造 Request 对象
        Request request = new Request();
        request.setType(0x5); // 创建队列请求
        request.setLength(payload.length);
        request.setPayload(payload);

        // 发送请求
        connection.wirteRequest(request);
        // 等待服务器响应
        BasicReturns basicReturns = waitResult(queueDeclareArguments.getRid());
        return basicReturns.isSuccess();
    }

    // 删除队列
    public boolean queueDelete(String queueName) throws IOException {
        QueueDeleteArguments queueDeleteArguments = new QueueDeleteArguments();
        queueDeleteArguments.setChannelId(channelId);
        queueDeleteArguments.setRid(generateRid());
        queueDeleteArguments.setQueueName(queueName);
        byte[] payload = BinaryTool.toBytes(queueDeleteArguments);

        // 构造 Request 对象
        Request request = new Request();
        request.setType(0x6); // 删除队列请求
        request.setLength(payload.length);
        request.setPayload(payload);

        // 发送请求
        connection.wirteRequest(request);
        // 等待服务器响应
        BasicReturns basicReturns = waitResult(queueDeleteArguments.getRid());
        return basicReturns.isSuccess();
    }

    // 创建绑定
    public boolean queueBind(String queueName, String exchangeName, String bindingKey) throws IOException {
        QueueBindArguments queueBindArguments = new QueueBindArguments();
        queueBindArguments.setChannelId(channelId);
        queueBindArguments.setRid(generateRid());
        queueBindArguments.setQueueName(queueName);
        queueBindArguments.setExchangeName(exchangeName);
        queueBindArguments.setBindingKey(bindingKey);
        byte[] payload = BinaryTool.toBytes(queueBindArguments);

        // 构造 Request 对象
        Request request = new Request();
        request.setType(0x7); // 创建绑定请求
        request.setLength(payload.length);
        request.setPayload(payload);

        // 发送请求
        connection.wirteRequest(request);
        // 等待服务器响应
        BasicReturns basicReturns = waitResult(queueBindArguments.getRid());
        return basicReturns.isSuccess();
    }

    // 删除绑定
    public boolean queueUnbind(String queueName, String exchangeName) throws IOException {
        QueueUnBindArguments queueUnBindArguments = new QueueUnBindArguments();
        queueUnBindArguments.setChannelId(channelId);
        queueUnBindArguments.setRid(generateRid());
        queueUnBindArguments.setQueueName(queueName);
        queueUnBindArguments.setExchangeName(exchangeName);
        byte[] payload = BinaryTool.toBytes(queueUnBindArguments);

        // 构造 Request 对象
        Request request = new Request();
        request.setType(0x8); // 删除绑定请求
        request.setLength(payload.length);
        request.setPayload(payload);

        // 发送请求
        connection.wirteRequest(request);
        // 等待服务器响应
        BasicReturns basicReturns = waitResult(queueUnBindArguments.getRid());
        return basicReturns.isSuccess();
    }

    // 发送消息
    public boolean basicPublish(String exchangeName, String routingKey, BasicProperties basicProperties, byte[] body) throws IOException {
        BasicPublishArguments basicPublishArguments = new BasicPublishArguments();
        basicPublishArguments.setChannelId(channelId);
        basicPublishArguments.setRid(generateRid());
        basicPublishArguments.setExchangeName(exchangeName);
        basicPublishArguments.setRoutingKey(routingKey);
        basicPublishArguments.setBasicProperties(basicProperties);
        basicPublishArguments.setBody(body);
        byte[] payload = BinaryTool.toBytes(basicPublishArguments);

        // 构造 Request 对象
        Request request = new Request();
        request.setType(0x9); // 发送消息请求
        request.setLength(payload.length);
        request.setPayload(payload);

        // 发送请求
        connection.wirteRequest(request);
        // 等待服务器响应
        BasicReturns basicReturns = waitResult(basicPublishArguments.getRid());
        return basicReturns.isSuccess();
    }

    // 订阅消息
    public boolean basicConsume(String queueName, boolean autoAck, Consumer consumer) throws MqException, IOException {
        // 先设置回调
        if (this.consumer != null) {
            throw new MqException("该 channel 已经设置过消费者的消息回调了，不能重复设置!");
        }
        this.consumer = consumer;

        BasicConsumeArguments arguments = new BasicConsumeArguments();
        arguments.setChannelId(channelId);
        arguments.setRid(generateRid());
        arguments.setQueueName(queueName);
        arguments.setAutoAck(autoAck);
        byte[] payload = BinaryTool.toBytes(arguments);

        // 构造 Request 对象
        Request request = new Request();
        request.setType(0xa); // 订阅消息请求
        request.setLength(payload.length);
        request.setPayload(payload);
        // 发送请求
        connection.wirteRequest(request);
        // 等待服务器响应
        BasicReturns basicReturns = waitResult(arguments.getRid());
        return basicReturns.isSuccess();
    }

    // 确认消息
    public boolean basicAck(String queueName, String messageId) throws IOException {
        BasicAckArguments basicAckArguments = new BasicAckArguments();
        basicAckArguments.setChannelId(channelId);
        basicAckArguments.setRid(generateRid());
        basicAckArguments.setQueueName(queueName);
        basicAckArguments.setMessageId(messageId);
        byte[] payload = BinaryTool.toBytes(basicAckArguments);

        // 构造 Request 对象
        Request request = new Request();
        request.setType(0xb); // 确认消息请求
        request.setLength(payload.length);
        request.setPayload(payload);

        // 发送请求
        connection.wirteRequest(request);
        // 等待服务器响应
        BasicReturns basicReturns = waitResult(basicAckArguments.getRid());
        return basicReturns.isSuccess();
    }
}
