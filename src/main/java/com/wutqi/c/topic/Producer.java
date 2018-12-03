package com.wutqi.c.topic;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;

/**
 * 生产者
 */
public class Producer {
    private static final String EXCHANGE_NAME = "topic_logs";
    // 路由关键字
    private static final String[] routingKeys = new String[]{
            "quick.orange.rabbit",
            "lazy.orange.elephant",
            "quick.orange.fox",
            "lazy.brown.fox",
            "quick.brown.fox",
            "quick.orange.male.rabbit",
            "lazy.orange.male.rabbit"};

    public static void main(String[] args) {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        Connection connection = null;
        Channel channel = null;
        try {
            connection = connectionFactory.newConnection();
            channel = connection.createChannel();
            //声明交换机
            channel.exchangeDeclare(EXCHANGE_NAME, "topic");

            //循环发送具有不同routing key的message
            for (String routingKey : routingKeys) {
                String message = routingKey + "--->biu~";
                channel.basicPublish(EXCHANGE_NAME, routingKey, null, message.getBytes());
                System.out.println("Producer -> routingkey: " + routingKey + ", send message " + message);
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
