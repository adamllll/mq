package org.adam.mq.mqclient;

public class ConnectionFactory {
    // broker server 的ip地址
    private String host;
    private int port;

    // 访问broker server 的哪个虚拟主机
    // 暂时不实现
//    private String virtualHostName;
//    private String userName;
//    private String password;
    public Connection newConnection() {
        Connection connection = new Connection(host, port);
        return connection;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }
}
