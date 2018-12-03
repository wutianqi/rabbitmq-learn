package com.wutqi.d.rpc;

import com.rabbitmq.client.*;

import java.io.IOException;

/**
 * rpc服务器
 */
public class RpcServer {
    private static final String RPC_QUEUE_NAME = "rpc_queue";

    private static String format(String message){
        return "......" + message + "......";
    }

    public static void main(String[] args) {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        Connection connection = null;
        try {
            connection = connectionFactory.newConnection();
            Channel channel = connection.createChannel();
            channel.queueDeclare(RPC_QUEUE_NAME,false,false,false,null);
            QueueingConsumer consumer = new QueueingConsumer(channel);
            //声明消费者预取的消息数量
            channel.basicQos(1);
            channel.basicConsume(RPC_QUEUE_NAME,false,consumer);//采用手动回复消息
            System.out.println("RpcServer waitting for receive message");

            while (true){
                //接收并处理消息
                QueueingConsumer.Delivery delivery = consumer.nextDelivery();
                String message = new String(delivery.getBody(), "UTF-8");
                System.out.println("RpcServer receive message " + message);
                String response = format(message);
                //确认收到消息
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(),false);

                //取出消息的correlationId
                AMQP.BasicProperties properties = delivery.getProperties();
                String correlationId = properties.getCorrelationId();

                //创建具有与接收消息相同的correlationId的消息属性
                AMQP.BasicProperties replyProperties = new AMQP.BasicProperties().builder().correlationId(correlationId).build();
                channel.basicPublish("",properties.getReplyTo(),replyProperties,response.getBytes());
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
