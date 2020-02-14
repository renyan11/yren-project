package com.fh.yren.activemq.provider;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

public class ProviderBroker {
    public static void main(String[] args) {
        ConnectionFactory connectFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
        Connection connection = null;
        Session session = null;
        MessageProducer producer = null;
        try {
            connection = connectFactory.createConnection();
            connection.start();
            session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("ren-yan-define-broker");
            producer = session.createProducer(queue);
            //持久化的消息
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);
            //非持久化的消息
            //producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
            for (int i = 1; i < 8; i++) {
                TextMessage textMessage = session.createTextMessage("Broker消息" + i);
                producer.send(textMessage);
            }
            //事务为true,必须要commit();false是自动提交
            session.commit();
        } catch (Exception e) {
            try {
                session.rollback();
            } catch (JMSException e1) {
                e1.printStackTrace();
            }
            e.printStackTrace();
        } finally {
            if (producer != null) {
                try {
                    producer.close();
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
