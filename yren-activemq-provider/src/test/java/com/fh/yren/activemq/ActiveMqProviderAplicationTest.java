package com.fh.yren.activemq;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.jms.*;

@RunWith(SpringRunner.class)
@SpringBootTest
public class ActiveMqProviderAplicationTest {


    public static void main(String[] args) {
        ActiveMqProviderAplicationTest aplicationTest = new ActiveMqProviderAplicationTest();
        //队列消息生产
        //aplicationTest.QueueProvider();
        //主题消息生产(非持久)
        //aplicationTest.TopicProvider();
        //主题消息生产(持久化)
        aplicationTest.TopicProviderPersistent();
    }

    /**
     * Queue Provider 队列 生产者
     */
    public void QueueProvider(){
        ConnectionFactory connectFactory = new ActiveMQConnectionFactory("tcp://101.133.232.101:61616");
        Connection connection = null;
        Session session = null;
        MessageProducer producer = null;
        try {
            connection = connectFactory.createConnection();
            connection.start();
            session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("ren-yan-queue-test");
            producer = session.createProducer(queue);
            //持久化的消息
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);
            //非持久化的消息
            //producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
            for (int i = 0; i < 5; i++) {
                TextMessage textMessage = session.createTextMessage("test队列消息" + i);
                producer.send(textMessage);
            }
            session.commit();
        } catch (Exception e) {
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

    /**
     * Topic Provider 主题 生产者 --- 非持久化
     */
    public void TopicProvider(){
        ConnectionFactory connectFactory = new ActiveMQConnectionFactory("tcp://101.133.232.101:61616");
        Connection connection = null;
        Session session = null;
        MessageProducer producer = null;
        try {
            connection = connectFactory.createConnection();
            connection.start();
            session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Topic topic = session.createTopic("ren-yan-topic-test");
            producer = session.createProducer(topic);
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);
            for (int i = 0; i < 5; i++) {
                TextMessage textMessage = session.createTextMessage("test主题消息" + i);
                producer.send(textMessage);
            }
            session.commit();
        } catch (Exception e) {
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

    /**
     * Topic Provider 主题 生产者 --- 持久化
     */
    public void TopicProviderPersistent() {
        ConnectionFactory connectFactory = new ActiveMQConnectionFactory("tcp://101.133.232.101:61616");
        Connection connection = null;
        Session session = null;
        MessageProducer producer = null;
        try {
            connection = connectFactory.createConnection();
            //connection.start();
            session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Topic topic = session.createTopic("ren-yan-topic-persistent");
            producer = session.createProducer(topic);
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);
            //启动位置
            connection.start();
            for (int i = 0; i < 5; i++) {
                TextMessage textMessage = session.createTextMessage("topic的持久化主题消息---再生产数据---" + i);
                producer.send(textMessage);
            }
            session.commit();
        } catch (Exception e) {
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
