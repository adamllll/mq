package org.adam.mq.mqclient;

import org.adam.mq.common.*;

import java.io.*;
import java.net.Socket;
import java.net.SocketException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Connection {
    private Socket socket = null;
    // 需要管理多个Channel 使用Hash表把这些Channel组织起来
    private ConcurrentHashMap<String, Channel> channelMap = new ConcurrentHashMap<>();

    private InputStream inputStream;
    private OutputStream outputStream;
    private DataInputStream dataInputStream;
    private DataOutputStream dataOutputStream;

    private ExecutorService callbackPool = null;

    public Connection(String host, int port) throws IOException {
        socket = new Socket(host, port);
        inputStream = socket.getInputStream();
        outputStream = socket.getOutputStream();
        dataInputStream = new DataInputStream(inputStream);
        dataOutputStream = new DataOutputStream(outputStream);

        callbackPool = Executors.newFixedThreadPool(4);

        // 创建一个扫描线程，不断地从socket中读取响应数据，再把响应数据分发给各个Channel
        Thread responseReaderThread = new Thread(() -> {
            while (!socket.isClosed()) {
                try {
                    Response response = readResponse();
                    dispatchResponse(response);
                } catch (SocketException e) {
                    // 连接正常断开，此时异常直接忽略
                    System.out.println("[Connection] 连接已关闭，停止读取响应线程");
                } catch (IOException | ClassNotFoundException | MqException e) {
                    System.out.println("[Connection] 读取响应时发生IO异常，停止读取响应线程");
                    e.printStackTrace();
                }
            }
        });
        responseReaderThread.start();
    }
    // 关闭连接,释放资源
    public void close() throws IOException {
        socket.close();
        callbackPool.shutdown();
        channelMap.clear();
        inputStream.close();
        outputStream.close();
        System.out.println("[Connection] 连接已关闭");
    }

    // 使用这个方法把响应分发给各个 Channel
    private void dispatchResponse(Response response) throws IOException, ClassNotFoundException, MqException {
        if (response.getType() == 0xc) {
            // 服务器推送来的数据
            SubscribeReturns subscribeReturns =(SubscribeReturns) BinaryTool.fromBytes(response.getPayload());
            // 找到对应的 Channel 来执行对应的回调方法
            Channel channel = channelMap.get(subscribeReturns.getChannelId());
            if (channel == null) {
                throw new MqException("[Connection] 找不到对应的 Channel，channelId=" + subscribeReturns.getChannelId());
            }
            callbackPool.submit(() -> {
                try {
                    channel.getConsumer().handleDelivery(subscribeReturns.getConsumerTag(), subscribeReturns.getBasicProperties(),
                            subscribeReturns.getBody());
                } catch (MqException | IOException e) {
                    System.out.println("[Connection] 处理订阅消息时发生异常，channelId=" + subscribeReturns.getChannelId());
                    e.printStackTrace();
                }
            });
        }else {
            // 针对控制请求的响应，需要找到对应的 Channel 来处理
            BasicReturns basicReturns = (BasicReturns) BinaryTool.fromBytes(response.getPayload());
            // 把结果放入对应 Channel 的 Hash表中
            Channel channel = channelMap.get(basicReturns.getChannelId());
            if (channel == null) {
                throw new MqException("[Connection] 找不到对应的 Channel，channelId=" + basicReturns.getChannelId());
            }
            channel.putReturns(basicReturns);
        }
    }

    // 发送请求
    public void wirteRequest(Request request) throws IOException {
        dataOutputStream.writeInt(request.getType());
        dataOutputStream.writeInt(request.getLength());
        dataOutputStream.write(request.getPayload());
        dataOutputStream.flush();
        System.out.println("[Connection] 发送请求，type=" + request.getType() + ", length=" + request.getLength());
    }
    // 读取响应
    public Response readResponse() throws IOException {
        Response response = new Response();
        response.setType(dataInputStream.readInt());
        response.setLength(dataInputStream.readInt());
        byte[] payload = new byte[response.getLength()];
        int n = dataInputStream.read(payload);
        if (n != response.getLength()) {
            throw new IOException("读取响应数据长度不匹配");
        }
        response.setPayload(payload);
        System.out.println("[Connection] 收到响应，type=" + response.getType() + ", length=" + response.getLength());
        return response;
    }
    // 通过这个方法创建一个新的 Channel
    public Channel createChannel() throws IOException {
        String channelId = "C" + UUID.randomUUID().toString();
        Channel channel = new Channel(channelId, this);
        // 把这个 Channel 放到 channelMap 里进行管理
        channelMap.put(channelId, channel);
        // 把创建 Channel 的消息告诉服务器
        boolean success = channel.createChannel();
        if (!success) {
            // 服务器创建 Channel 失败，从 channelMap 里移除这个 Channel
            System.out.println("[Connection] 创建 Channel 失败，channelId=" + channelId);
            channelMap.remove(channelId);
            return null;
        }
        System.out.println("[Connection] 创建新的 Channel，channelId=" + channelId);
        return channel;
    }
}
