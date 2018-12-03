package com.wutqi.a.direct;

import com.rabbitmq.client.*;

import java.io.IOException;

/**
 * 消息消费者1
 */
public class Consumer1 {
    private static final String EXCHANGE_NAME = "direct_logs";
    // 路由关键字
    private static final String[] routingKeys = new String[]{"info", "warning"};

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
            connection = connectionFactory.newConnection();
            channel = connection.createChannel();
            //声明队列，这里队列的名字由代理自动生成
            String queueName = channel.queueDeclare().getQueue();
            //声明交换机
            //参数1：交换机名字，参数2：交换机类型
            channel.exchangeDeclare(EXCHANGE_NAME, "direct");

            for (String routingKey : routingKeys) {
                //将交换机和队列用routing key绑定起来
                //参数1：队列名，参数2：交换机名，参数3：队列和交换机之间绑定的路由键
                channel.queueBind(queueName, EXCHANGE_NAME, routingKey);
                System.out.println("Consumer1 -> queue: " + queueName + ", exchange_name: " + EXCHANGE_NAME + ", routingKey: " + routingKey);
            }

            //声明消费者
            QueueingConsumer consumer = new QueueingConsumer(channel);
            //指定从哪个消费者从哪个通道获取消息，并指明自动确认的机制
            //参数1：队列名，参数2：确认机制，true表示自动确认，false代表手动确认，参数3：消费者
            channel.basicConsume(queueName, true, consumer);
            System.out.println("Consumer1 waitting for message");
            
            while (true){
                //获取消息，这一步会一直阻塞，直到收到消息
                QueueingConsumer.Delivery delivery = consumer.nextDelivery();
                //获取消息
                String message = new String(delivery.getBody(), "UTF-8");
                System.out.println("Consumer1 receive message " + message);
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
