package com.wutqi.d.rpc;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.UUID;

/**
 * rpc客户端
 */
public class RpcClient {
    //发送消息的队列名称
    private static final String RPC_QUEUE_NAME = "rpc_queue";

    public static void main(String[] args) {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        Connection connection = null;
        Channel channel = null;
        try {
           connection = connectionFactory.newConnection();
           channel = connection.createChannel();
           //创建回调队列
           String callbackQueue = channel.queueDeclare().getQueue();
           //创建回调队列，消费者从回调队列中接收服务端传送的消息
            QueueingConsumer consumer = new QueueingConsumer(channel);
            channel.basicConsume(callbackQueue,true,consumer);

            //创建消息带有correlationId的消息属性
            String correlationId = UUID.randomUUID().toString();
            AMQP.BasicProperties basicProperties = new AMQP.BasicProperties().builder().correlationId(correlationId).replyTo(callbackQueue).build();
            String message = "hello rabbitmq";
            channel.basicPublish("",RPC_QUEUE_NAME,basicProperties,message.getBytes());
            System.out.println("RpcClient send message " + message + ", correaltionId = " + correlationId);

            //接收回调消息
            while (true){
                QueueingConsumer.Delivery delivery = consumer.nextDelivery();
                String receivedCorrelationId = delivery.getProperties().getCorrelationId();
                if(correlationId.equals(receivedCorrelationId)){
                    System.out.println("RpcClient receive format message " + new String(delivery.getBody(), "UTF-8") + ", correaltionId = " + correlationId);
                    break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
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
