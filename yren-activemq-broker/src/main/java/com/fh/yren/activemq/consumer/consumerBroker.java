package com.fh.yren.activemq.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

@Slf4j
public class consumerBroker {
    public static void main(String[] args) {
        ConnectionFactory connectFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
        Connection connection = null;
        Session session = null;
        MessageConsumer consumer = null;
        try {
            connection = connectFactory.createConnection();
            connection.start();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("ren-yan-define-broker");
            consumer = session.createConsumer(queue);
            consumer.setMessageListener((message -> {
                TextMessage textMessage = (TextMessage) message;
                try {
                    log.info("我是Broker消费者,接收到消息 = " + textMessage.getText());
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }));
            System.in.read();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (consumer != null) {
                try {
                    consumer.close();
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
            if (session != null) {
                try {
                    session.close();
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
