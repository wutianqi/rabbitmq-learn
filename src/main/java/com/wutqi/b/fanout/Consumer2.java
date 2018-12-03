package com.wutqi.b.fanout;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

import java.io.IOException;

/**
 * 消费者2
 */
public class Consumer2 {
    public final static String EXCHANGE_NAME = "logs";

    public static void main(String[] args) {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        Connection connection = null;
        Channel channel = null;
        try {
            connection = connectionFactory.newConnection();
            channel = connection.createChannel();
            String queueName = channel.queueDeclare().getQueue();
            //声明一个交换机，发布模式为fanout-扇形
            channel.exchangeDeclare(EXCHANGE_NAME,"fanout");
            //将队列和交换机绑定起来
            channel.queueBind(queueName,EXCHANGE_NAME,"");

            QueueingConsumer consumer = new QueueingConsumer(channel);
            //当连接断开时，队列会自动被删除
            channel.basicConsume(queueName,true,consumer);
            System.out.println("Consumer2 Waitting for message");
            while (true){
                QueueingConsumer.Delivery delivery = consumer.nextDelivery();
                String message = new String(delivery.getBody(), "UTF-8");
                System.out.println("Consumer2 receives message " + message);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
