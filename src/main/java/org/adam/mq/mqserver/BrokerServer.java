package org.adam.mq.mqserver;

import org.adam.mq.common.*;
import org.adam.mq.mqserver.core.BasicProperties;

import java.io.*;
import java.io.EOFException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 这个 BrokerServer 就是消息队列的本体服务器
 * 本质上就是一个TCP服务器，负责监听客户端的连接请求
 */
public class BrokerServer {
    private ServerSocket serverSocket = null;

    // 当前只考虑一个 BrokerServer 对应一个 VirtualHost 的情况
    private VirtualHost virtualHost = new VirtualHost("DefaultVHost");
    // 使用这个 ConcurrentHashMap 来保存所有已经连接的会话
    // 此处的key是ChannelId 的字符串表示，value是对应的Socket对象
    private ConcurrentHashMap<String, Socket> sessions = new ConcurrentHashMap<String, Socket>();
    // 引入线程池来处理多个客户端的请求
    private ExecutorService executorService = null;
    // 引入 boolean 标志来控制服务器的运行状态
    private volatile boolean isRunning = true;

    public BrokerServer(int port) throws IOException {
        serverSocket = new ServerSocket(port);
    }

    public void start() throws IOException {
        System.out.println("[BrokerServer] 启动，等待客户端连接...");
        executorService = Executors.newCachedThreadPool(); // 使用缓存线程池
        while (isRunning) {
            Socket clientSocket = serverSocket.accept();
            // 把处理连接的逻辑交给线程池来处理
            executorService.submit(() -> {
                try {
                    processConnection(clientSocket);
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }
    // 一般来说，停止服务器是一个优雅关闭的过程？ kill掉对应进程就行~
    public void stop() throws IOException {
        isRunning = false;
        if (serverSocket != null && !serverSocket.isClosed()) {
            serverSocket.close();
        }
        if (executorService != null && !executorService.isShutdown()) {
            // 把线程池中的所有线程都强制停止
            executorService.shutdownNow();
        }
        System.out.println("[BrokerServer] 已停止。");
    }
    // 处理客户端连接的具体逻辑
    // 在一个连接中，可能会涉及到多个请求和响应
    private void processConnection(Socket clientSocket) throws ClassNotFoundException {
        try (InputStream inputStream = clientSocket.getInputStream();
             OutputStream outputStream = clientSocket.getOutputStream();
             DataInputStream dataInputStream = new DataInputStream(inputStream);
             DataOutputStream dataOutputStream = new DataOutputStream(outputStream)) {
            // 需要按照特定格式来读取解析，就需要用到DataInputStream 和 DataOutputStream
            while (true) {
                // 1. 读取请求并解析
                Request request = readRequest(dataInputStream);
                // 2. 根据请求计算响应
                Response response = process(request, clientSocket);
                // 3. 把响应写回到客户端
                writeResponse(dataOutputStream, response);
            }
        } catch (EOFException e) {
            // 客户端正常关闭连接（DataInputStream读到EOF就抛出异常借助这个异常来结束循环）
            System.out.println("[BrokerServer] connection关闭，客户端:"+ clientSocket.getInetAddress().toString() +
                    ",端口号:"+ clientSocket.getPort() +"已断开连接。");
        } catch (IOException | MqException e) {
            System.out.println("[BrokerServer] 处理连接时发生错误: " + e.getMessage());
            e.printStackTrace();
        } finally {
            try {
                // 当前连接处理完毕，关闭连接
                if (!clientSocket.isClosed()) {
                    clientSocket.close();
                }
                // 一个TCP连接可能包含多个Channel，会话的关闭需要把对应的ChannelId从sessions中移除
                clearClosedSession(clientSocket);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private Request readRequest(DataInputStream dataInputStream) throws IOException {
        Request request = new Request();
        request.setType(dataInputStream.readInt()); // 读取请求类型
        request.setLength(dataInputStream.readInt()); // 读取请求数据长度
        byte[] payload = new byte[request.getLength()]; // 读取请求数据内容
        int n = dataInputStream.read(payload);
        if (n != request.getLength()) {
            throw new IOException("读取请求数据长度不匹配，期望长度: " + request.getLength() + ", 实际长度: " + n);
        }
        request.setPayload(payload);
        return request;
    }
    private void writeResponse(DataOutputStream dataOutputStream, Response response) throws IOException {
        dataOutputStream.writeInt(response.getType()); // 写入响应类型
        dataOutputStream.writeInt(response.getLength()); // 写入响应数据长度
        dataOutputStream.write(response.getPayload()); // 写入响应数据内容
        dataOutputStream.flush(); // 刷新输出流，确保数据发送
    }
    private Response process(Request request, Socket clientSocket) throws IOException, ClassNotFoundException, MqException {
        // 1. 把 request 中的payload 做一个反序列化的解析
        BasicArguments basicArguments = (BasicArguments) BinaryTool.fromBytes(request.getPayload());
        System.out.println("[BrokerServer] 收到请求，" +
                "rid: " + basicArguments.getRid() +
                "channelId: " + basicArguments.getChannelId() +
                "类型: " + request.getType() +
                ", 来自客户端: " + clientSocket.getInetAddress().toString() +
                ", 端口号: " + clientSocket.getPort() +
                ", 长度: " + request.getLength());
        // 2. 根据请求type进行不同的处理
        boolean sucess = true;
        if (request.getType() == 0x1 ) {
            // 创建 channel
            sessions.put(basicArguments.getChannelId(), clientSocket);
            System.out.println("[BrokerServer] 创建 channel 成功，channelId: " + basicArguments.getChannelId());
        } else if (request.getType() == 0x2) {
            // 销毁 channel
            sessions.remove(basicArguments.getChannelId());
            System.out.println("[BrokerServer] 销毁 channel 成功，channelId: " + basicArguments.getChannelId());
        } else if (request.getType() == 0x3) {
            // 创建交换机，此时 payload就是 ExchangeDeclareArguments 对象
            ExchangeDeclareArguments arguments = (ExchangeDeclareArguments) basicArguments;
            sucess = virtualHost.exchangeDeclare(
                    arguments.getExchangeName(),
                    arguments.getExchangeType(),
                    arguments.isDurable(),
                    arguments.isAutoDelete(),
                    arguments.getArguments());
        } else if (request.getType() == 0x4) {
            // 销毁交换机
            ExchangeDeclareArguments arguments = (ExchangeDeclareArguments) basicArguments;
            sucess = virtualHost.exchangeDelete(arguments.getExchangeName());
        } else if (request.getType() == 0x5) {
            // 创建队列
            QueueDeclareArguments arguments = (QueueDeclareArguments) basicArguments;
            sucess = virtualHost.queueDeclare(
                    arguments.getQueueName(),
                    arguments.isDurable(),
                    arguments.isExclusive(),
                    arguments.isAutoDelete(),
                    arguments.getArguments());
        } else if (request.getType() == 0x6) {
            // 销毁队列
            QueueDeclareArguments arguments = (QueueDeclareArguments) basicArguments;
            sucess = virtualHost.queueDelete(arguments.getQueueName());
        } else if (request.getType() == 0x7) {
            // 创建绑定
            QueueBindArguments arguments = (QueueBindArguments) basicArguments;
            sucess = virtualHost.queueBind(arguments.getQueueName(), arguments.getExchangeName(),arguments.getBindingKey());
        } else if (request.getType() == 0x8) {
            // 销毁绑定
            QueueBindArguments arguments = (QueueBindArguments) basicArguments;
            sucess = virtualHost.queueUnbind(arguments.getQueueName(), arguments.getExchangeName());
        } else if (request.getType() == 0x9) {
            // 发布消息
            BasicPublishArguments arguments = (BasicPublishArguments) basicArguments;
            sucess = virtualHost.basicPublish(
                    arguments.getExchangeName(),
                    arguments.getRoutingKey(),
                    arguments.getBasicProperties(),
                    arguments.getBody());
        } else if (request.getType() == 0xa) {
            // 订阅消息
            BasicConsumeArguments arguments = (BasicConsumeArguments) basicArguments;
            sucess = virtualHost.basicConsume(
                    arguments.getConsumerTag(),
                    arguments.getQueueName(),
                    arguments.isAutoAck(),
                    new Consumer() {
                        // 此处的回调函数把服务器收到的消息直接推送给对应的消费者客户端
                        @Override
                        public void handleDelivery(String consumerTag, BasicProperties properties, byte[] body) throws MqException, IOException {
                            // 先知道当前这个收到的消息需要发给哪个客户端
                            // 此处的 consumerTag 就是 channelId,根据 channelId去 session中查询对应的 Socket对象
                            // 1. 根据 channelId找到 socket 对象
                            Socket clinetSocket = sessions.get(consumerTag);
                            if (clinetSocket == null || clinetSocket.isClosed()) {
                                throw new MqException("[BrokerServer] 发送消息失败，未找到对应的客户端连接，consumerTag: " + consumerTag);
                            }
                            // 2. 构造响应数据
                            SubscribeReturns subscribeReturns = new SubscribeReturns();
                            subscribeReturns.setChannelId(consumerTag);
                            subscribeReturns.setRid(""); // 此处不需要 rid，可以设置为空字符串(只有响应没有请求)
                            subscribeReturns.setSuccess(true);
                            subscribeReturns.setConsumerTag(consumerTag);
                            subscribeReturns.setBasicProperties(properties);
                            subscribeReturns.setBody(body);
                            Response response = new Response();
                            byte[] payload = BinaryTool.toBytes(subscribeReturns);
                            response.setType(0xc); // 消息推送的响应类型 0xc表示消费者给客户端推送的消息
                            // response的 payload 是 SubscribeReturns 对象
                            response.setLength(payload.length);
                            request.setPayload(payload);
                            // 3. 把响应写回到客户端 注意此处的DataOutputStream 不能关闭，否则会关闭 socket 连接
                            DataOutputStream dataOutputStream = new DataOutputStream(clinetSocket.getOutputStream());
                            writeResponse(dataOutputStream, response);
                        }
                    });

        } else if (request.getType() == 0xb) {
            // 调用 basicAck 确认消息
            BasicAckArguments arguments = (BasicAckArguments) basicArguments;
            sucess = virtualHost.basicAck(arguments.getQueueName(), arguments.getMessageId());
        } else {
            // 当前的 type 是非法的
            throw new MqException("[BrokerServer] 收到非法的请求类型: " + request.getType());
        }
        // 3. 构造响应对象并返回
        BasicReturns basicReturns = new BasicReturns();
        basicReturns.setChannelId(basicArguments.getChannelId());
        basicReturns.setRid(basicArguments.getRid());
        basicReturns.setSuccess(sucess);
        // 构造响应对象
        Response response = new Response();
        byte[] payload = BinaryTool.toBytes(basicReturns);
        response.setType(request.getType()); // 响应类型和请求类型相同
        response.setLength(payload.length);
        response.setPayload(payload);
        System.out.println("[BrokerServer] 处理请求完成，rid: " + basicArguments.getRid() +
                ", channelId: " + basicArguments.getChannelId() +
                ", 类型: " + request.getType() +
                ", 成功: " + sucess);
        return response;
    }
    
    private void clearClosedSession(Socket clientSocket) {
        List<String> toDeletechannelId = new ArrayList<>();
        for (Map.Entry<String, Socket> entry : sessions.entrySet()) {
            if (entry.getValue().equals(clientSocket)) {
                // 不能直接删除，否则会引发 ConcurrentModificationException（一边遍历一边删除）
                //sessions.remove(entry.getKey());
                toDeletechannelId.add(entry.getKey());
            }
        }
        for (String channelId : toDeletechannelId) {
            sessions.remove(channelId);
            System.out.println("[BrokerServer] 会话关闭，移除 channelId: " + toDeletechannelId);
        }
    }
}
