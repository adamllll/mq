package org.adam.mq.common;

import java.io.Serializable;

/**
 * 使用这个类表示方法的公共参数/辅助的字段
 * 后续每个方法又会有一些不同的参数，不同的参数再分别使用不同的子类来表示
 */
public class BasicArguments implements Serializable {
    protected String rid; // 请求ID，用于唯一标识一次请求,用于请求和响应的对应
    protected String channelId; // 通信通道ID，用于标识通信的通道

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
