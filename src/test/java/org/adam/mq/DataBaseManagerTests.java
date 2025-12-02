package org.adam.mq;

import org.adam.mq.mqserver.datacenter.DataBaseManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.test.context.SpringBootTest;

// 加上这个注解表示这是一个Spring Boot的测试类
// 设计单元测试，要求单元测试用例之间是相互独立的，不会互相干扰
@SpringBootTest
public class DataBaseManagerTests {
    private DataBaseManager dbManager = new DataBaseManager();

    // 下面的每一个方法都是一个/一组测试用例
    // 还需要做一个准备工作，需要写两个方法，分别用于进行“准备工作”和“收尾工作”

    // 使用这个方法，来执行准备工作，每个用例执行前，都需要调用这个方法
    @BeforeEach
    public void setUp() {
        // 由于init()中需要通过 context对象拿到 metaMapper实例
        // 所以就需要先把context对象初始化好
        MqApplication.context = SpringApplication.run(MqApplication.class);
        dbManager.init();
    }
    // 使用这个方法，来执行收尾工作，每个用例执行后，都需要调用这个方法
    @AfterEach
    public void tearDown() {
        // 把数据库文件删除掉，以保证每个用例都是在一个干净的环境下运行的
        // 此处不能直接删除，而需要先关闭上述 context 对象
        // 此处的 context 对象，持有了 MetaMapper的实例，而 MetaMapper实例持有了数据库连接(打开了meta.db文件)
        // 如果 meta.db文件正在被使用，那么就无法删除这个文件
        MqApplication.context.close();
        dbManager.deleteDB();
    }
}
