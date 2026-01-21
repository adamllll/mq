package org.adam.mq.mqclient;

import org.adam.mq.common.Request;
import org.adam.mq.common.Response;

import java.io.*;
import java.net.Socket;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class Connection {
    private Socket socket = null;
    // 需要管理多个Channel 使用Hash表把这些Channel组织起来
    private ConcurrentHashMap<String, Channel> channelMap = new ConcurrentHashMap<>();

    private InputStream inputStream;
    private OutputStream outputStream;
    private DataInputStream dataInputStream;
    private DataOutputStream dataOutputStream;

    public Connection(String host, int port) throws IOException {
        socket = new Socket(host, port);
        inputStream = socket.getInputStream();
        outputStream = socket.getOutputStream();
        dataInputStream = new DataInputStream(inputStream);
        dataOutputStream = new DataOutputStream(outputStream);
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
