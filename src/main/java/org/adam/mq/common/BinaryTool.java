package org.adam.mq.common;

import java.io.*;

/**
 * 这个类提供了一些用于处理二进制数据的工具方法。
 */
// 下列的逻辑，并不仅仅是 Message，其他的Java中的对象，也是可以通过这样的逻辑进行序列化和反序列化的
// 如果要想让这个对象支持序列化，那么这个对象所属的类，就必须实现 Serializable 接口
public class BinaryTool {
    // 把一个对象转换为字节数组
    public static byte[] toBytes(Object object) throws IOException {
        // 这个流对象相当于一个变长的字节数组
        // 就可以把 object 序列化的数据给逐渐的写入到 byteArrayOutputStream 中，再统一转成 byte[]
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try (ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream)) {
            // 此处的 wirteObject 方法会把 object 对象序列化后写入到 ObjectOutStream中
            // 由于 ObjectOutStream 底层连接的是 ByteArrayOutputStream
            // 所以最终数据会写入到 byteArrayOutputStream 中
            objectOutputStream.writeObject(object);
            objectOutputStream.flush();
            // 这个操作就是把 byteArrayOutputStream 中持有的二进制数据取出来，转成byte[]
            // 从 byteArrayOutputStream 中获取最终的字节数组
            return byteArrayOutputStream.toByteArray();
        }
    }

    // 从字节数组还原为对象
    public static Object fromBytes(byte[] data) throws IOException, ClassNotFoundException {
        Object object = null;
        try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data)) {
            try (ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream)) {
                // 此处的 readObject就是从 data这个字节数组byte[]中还原出对象(读取数据并进行反序列化)
                object = objectInputStream.readObject();
            }
        }
        return object;
    }

}
