package com.wutqi.a.direct;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;

/**
 * 消息发送者
 */
public class LogProducer {
    //交换机名字
    private static final String EXCHANGE_NAME = "direct_logs";
    // 路由关键字
    private static final String[] routingKeys = new String[]{"info" ,"warning", "error"};

    public static void main(String[] args) {
        //创建连接工厂并设置连接信息，这些连接信息都是默认的，所以这里注释掉了，打开也是可以的。
        ConnectionFactory connectionFactory = new ConnectionFactory();
//        connectionFactory.setHost("localhost");
//        connectionFactory.setUsername("guest");
//        connectionFactory.setPassword("guest");
//        connectionFactory.setPort(5672);
        Connection connection = null;
        Channel channel = null;
        try {
            //获取连接
            connection = connectionFactory.newConnection();
            //连接中打开通道
            channel = connection.createChannel();
            //声明交换机
            //参数1：交换机名字，参数2：交换机类型
            channel.exchangeDeclare(EXCHANGE_NAME, "direct");

            for (String routingKey : routingKeys){
                //将消息发送给交换机，我们这里发送的消息就是routingKey
                //参数1：交换机名字，参数2：消息路由键，参数3：消息属性，参数4：消息体
                channel.basicPublish(EXCHANGE_NAME, routingKey,null, routingKey.getBytes());
                System.out.println("LogProducer -> routingkey: " + routingKey + ", send message " + routingKey);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                channel.close();
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }
}
