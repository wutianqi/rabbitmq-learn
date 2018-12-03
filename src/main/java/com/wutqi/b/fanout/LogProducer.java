package com.wutqi.b.fanout;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;

/**
 * 生产者
 */
public class LogProducer {
    //交换机名称
    public final static String EXCHANGE_NAME = "logs";

    public static void main(String[] args) {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        Connection connection = null;
        Channel channel = null;
        try {
            connection = connectionFactory.newConnection();
            channel = connection.createChannel();
            channel.exchangeDeclare(EXCHANGE_NAME,"fanout");

            for (int i = 0; i < 5;i++){
                String message = "Hello Rabbit " + i;
                channel.basicPublish(EXCHANGE_NAME,"",null,message.getBytes());
                System.out.println("LogProducer send message " + message);
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
