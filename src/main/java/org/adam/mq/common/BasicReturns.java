package org.adam.mq.common;

import java.io.Serializable;

/**
 * 这个类表示各个远程调用的方法的返回值的公共信息
 */
public class BasicReturns implements Serializable {
    protected String rid; // 请求ID，用于唯一标识一次请求,用于请求和响应的对应
    protected String channelId; // 通信通道ID，用于标识通信的通道
    protected boolean success; // 方法调用是否成功

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public String getRid() {
        return rid;
    }

    public void setRid(String rid) {
        this.rid = rid;
    }

    public String getChannelId() {
        return channelId;
    }

    public void setChannelId(String channelId) {
        this.channelId = channelId;
    }

}
