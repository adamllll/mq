package org.adam.mq.common;

import java.io.Serializable;

public class ExchangeDeleteArguments extends BasicArguments implements Serializable {
    private String exchangeName; // 交换机名称

    public String getExchangeName() {
        return exchangeName;
    }

    public void setExchangeName(String exchangeName) {
        this.exchangeName = exchangeName;
    }
}
