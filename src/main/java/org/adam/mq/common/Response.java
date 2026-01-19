package org.adam.mq.common;

/**
 * 表示一个网络通信的响应对象，按照自定义应用层协议来展开
 */
public class Response {
    private int type; // 响应类型
    private int length; // 数据长度
    private byte[] payload; // 数据内容

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public byte[] getPayload() {
        return payload;
    }

    public void setPayload(byte[] payload) {
        this.payload = payload;
    }
}
