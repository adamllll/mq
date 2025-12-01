package org.adam.mq.mqserver.mapper;

import org.adam.mq.mqserver.core.Binding;
import org.adam.mq.mqserver.core.Exchange;
import org.adam.mq.mqserver.core.MSGQueue;
import org.apache.ibatis.annotations.Mapper;
import java.util.List;

@Mapper
public interface MetaMapper {
    // 提供三个核心建表方法
    void createExchangeTable();
    void createQueueTable();
    void createBindingTable();

    // 针对上述三个基本概念，进行，插入，删除
    void insertExchange(Exchange exchange);
    void deleteExchange(String exchangeName);

    void insertQueue(MSGQueue queue);
    void deleteQueue(String queueName);

    void insertBinding(Binding binding);
    void deleteBinding(Binding binding);

    List<Exchange> selectAllExchanges();
    List<MSGQueue> selectAllQueues();
    List<Binding> selectAllBindings();
}
