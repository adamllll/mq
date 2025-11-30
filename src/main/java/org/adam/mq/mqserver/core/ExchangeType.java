package org.adam.mq.mqserver.core;

public enum ExchangeType {
    DIRECT(0),
    FANOUT(1),
    TOPIC(2);
    private final int type;

    // 构造方法
    private  ExchangeType(int type) {
        this.type = type;
    }
    public int getType() {
        return type;
    }
}
