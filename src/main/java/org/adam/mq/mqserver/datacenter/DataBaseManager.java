package org.adam.mq.mqserver.datacenter;

import org.adam.mq.MqApplication;
import org.adam.mq.mqserver.core.Binding;
import org.adam.mq.mqserver.core.Exchange;
import org.adam.mq.mqserver.core.ExchangeType;
import org.adam.mq.mqserver.core.MSGQueue;
import org.adam.mq.mqserver.mapper.MetaMapper;

import java.io.File;
import java.util.List;

/**
 * 通过这个类来整合数据库的各种操作
 */
public class DataBaseManager {
    // 要做的是从 Spring中拿到现成的对象
    private MetaMapper metaMapper;

    // 针对数据库进行初始化
    // 构造方法一般是用来初始化类的属性的，一般的不太会涉及到太多的业务逻辑
    // 但此处的 init 方法是用来进行数据库的初始化操作的
    public void init() { // 初始化数据库 = 建库建表 + 插入一些默认的数据
        // 手动的获取 MetaMapper 对象
        metaMapper = MqApplication.context.getBean(MetaMapper.class);
        if (!checkDBExists()) {
            // 如果数据库不存在，则创建数据库和表，并插入默认数据
            createTable();
            // 插入默认数据
            insertDefaultData();
            System.out.println("[DBManager] Database and tables created, default data inserted.");
        }else {
            System.out.println("[DBManager] Database and tables already exists.");
        }
    }

    public void deleteDB() {
        File file = new File("./data/meta.db");
        if (file.exists()) {
            file.delete();
            System.out.println("[DBManager] Database file deleted successfully.");
        } else {
            System.out.println("[DBManager] Database file does not exist.");
        }
    }

    private boolean checkDBExists() {
        File file = new File("./data/meta.db");
        if (!file.exists()) {
            return false;
        }
        return true;
    }
    // 建表操作，建库操作不需要手动执行（不需要手动创建meta.db文件）
    // 因为只要连接数据库时，数据库文件不存在，SQLite会自动创建数据库文件
    private void createTable() {
        metaMapper.createExchangeTable();
        metaMapper.createQueueTable();
        metaMapper.createBindingTable();
        System.out.println("[DBManager] Tables created successfully.");
    }

    // 插入默认数据
    // 此处主要是添加一个默认的交换机 "default_exchange"
    // RabbitMQ中默认的交换机是一个特殊的交换机，名称为空字符串"",类型是DIRECT
    private void insertDefaultData() {
        // 构造一个默认的交换机
        Exchange exchange = new Exchange();
        exchange.setName("");
        exchange.setType(ExchangeType.DIRECT);
        exchange.setDurable(true);
        exchange.setAutoDelete(false);
        metaMapper.insertExchange(exchange);
        System.out.println("[DBManager] Default exchange inserted successfully.");
    }

    // 把其他的数据库操作，比如增删改查的方法，也放到这个类中来进行统一管理
    public void insertExchange(Exchange exchange) {
        metaMapper.insertExchange(exchange);
    }
    public void deleteExchange(String exchangeName) {
        metaMapper.deleteExchange(exchangeName);
    }
    public List<Exchange> selectAllExchanges() {
        return metaMapper.selectAllExchanges();
    }

    public void insertQueue(MSGQueue queue) {
        metaMapper.insertQueue(queue);
    }
    public  void deleteQueue(String queueName) {
        metaMapper.deleteQueue(queueName);
    }
    public List<MSGQueue> selectAllQueues() {
        return metaMapper.selectAllQueues();
    }

    public void insertBinding(Binding binding) {
        metaMapper.insertBinding(binding);
    }
    public void deleteBinding(Binding binding) {
        metaMapper.deleteBinding(binding);
    }
    public List<Binding> selectAllBindings() {
        return metaMapper.selectAllBindings();
    }

}
