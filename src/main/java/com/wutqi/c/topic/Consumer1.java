package com.wutqi.c.topic;

import com.rabbitmq.client.*;

import java.io.IOException;

/**
 * 消费者1
 */
public class Consumer1 {
    private static final String EXCHANGE_NAME = "topic_logs";
    // 路由关键字
    private static final String[] routingKeys = new String[]{"*.orange.*"};

    public static void main(String[] args) {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        Connection connection = null;
        Channel channel = null;
        try {
            connection = connectionFactory.newConnection();
            channel = connection.createChannel();
            //声明队列
            String queueName = channel.queueDeclare().getQueue();
            //声明交换机
            channel.exchangeDeclare(EXCHANGE_NAME, "topic");

            //将队列与交换器用routingkey绑定起来
            for (String routingKey : routingKeys) {
                channel.queueBind(queueName, EXCHANGE_NAME, routingKey);
                System.out.println("Consumer1 -> queue: " + queueName + ", exchange_name: " + EXCHANGE_NAME + ", routingKey: " + routingKey);
            }

            //接收消息
            QueueingConsumer consumer = new QueueingConsumer(channel);
            channel.basicConsume(queueName, true, consumer);
            System.out.println("Consumer1 waitting for message");

            while (true){
                QueueingConsumer.Delivery delivery = consumer.nextDelivery();
                String message = new String(delivery.getBody(), "UTF-8");
                Envelope envelope = delivery.getEnvelope();
                String routingKey = envelope.getRoutingKey();
                System.out.println("Consumer1 receive message " + message + ", routingKey: " + routingKey);
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
