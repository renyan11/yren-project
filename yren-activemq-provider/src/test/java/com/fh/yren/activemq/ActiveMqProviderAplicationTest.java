package com.fh.yren.activemq;

import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.jms.*;

@RunWith(SpringRunner.class)
@SpringBootTest
@Slf4j
public class ActiveMqProviderAplicationTest {


    public static void main(String[] args) {
        ActiveMqProviderAplicationTest aplicationTest = new ActiveMqProviderAplicationTest();
        //队列消息生产
        //aplicationTest.QueueProvider();
        //主题消息生产(非持久)
        //aplicationTest.TopicProvider();
        //主题消息生产(持久化)
        //aplicationTest.TopicProviderPersistent();
        //aplicationTest.testTopicRunning();
        //AMQ集群故障迁移验证
        aplicationTest.activemqBatchFailOver();
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

    /**
     * Topic Provider 主题 生产者 --- 非持久化
     */
    public void TopicProvider() throws JMSException {
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
            session.rollback();
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

    private static int time = 0;

    public void testTopicRunning(){
        ConnectionFactory connectFactory = new ActiveMQConnectionFactory("tcp://101.133.232.101:61616");
        Connection connection = null;
        Session session = null;
        MessageProducer producer = null;
        try {
            connection = connectFactory.createConnection();
            connection.start();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Topic topic = session.createTopic("ren-yan-topic-test-20200214-one");
            producer = session.createProducer(topic);
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);
            while(time < 200){
                time = time + 1;
                Thread.sleep(100);
                log.info("生产者发送消息..."+time);
                TextMessage textMessage = session.createTextMessage("主题消息生产者一直启动中：" + time);
                producer.send(textMessage);
            }
            //session.commit();
        } catch (Exception e) {
//            try {
//                session.rollback();
//            } catch (JMSException e1) {
//                e1.printStackTrace();
//            }
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


    public void activemqBatchFailOver() {
        String url = "failover:(tcp://101.133.232.101:61616,tcp://101.133.232.101:61617,tcp://101.133.232.101:61618)?randomize=false";
        ConnectionFactory connectFactory = new ActiveMQConnectionFactory(url);
        Connection connection = null;
        Session session = null;
        MessageProducer producer = null;
        try {
            connection = connectFactory.createConnection();
            connection.start();
            session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("ren-yan-amq-cluster");
            producer = session.createProducer(queue);
            //持久化的消息
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);
            //非持久化的消息
            //producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
            for (int i = 0; i < 5; i++) {
                TextMessage textMessage = session.createTextMessage("集群队列消息发出" + i);
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
