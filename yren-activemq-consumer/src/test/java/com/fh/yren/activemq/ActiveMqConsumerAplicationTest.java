package com.fh.yren.activemq;

import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.jms.*;

@RunWith(SpringRunner.class)
@SpringBootTest
@Slf4j
public class ActiveMqConsumerAplicationTest {

    ConnectionFactory connectFactory = new ActiveMQConnectionFactory("tcp://101.133.232.101:61616");
    Connection connection = null;
    Session session = null;
    MessageConsumer consumer = null;

    public static void main(String[] args) {
        ActiveMqConsumerAplicationTest aplicationTest = new ActiveMqConsumerAplicationTest();
        //队列消费者们
        //aplicationTest.consumerQueueTest1();
        //aplicationTest.consumerQueueTest2();
        //主题消费者们(非持久化)
        //aplicationTest.consumerTopicTest1();
        //aplicationTest.consumerTopicTest2();
        //主题消费者们(持久化)
        aplicationTest.consumerTopicPersistent();

    }

    @Test
    public void consumerQueueTest1() {
        try {
            connection = connectFactory.createConnection();
            connection.start();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("ren-yan-queue-test");
            consumer = session.createConsumer(queue);
            consumer.setMessageListener((message -> {
                TextMessage textMessage = (TextMessage) message;
                try {
                    log.info("我是队列消费者1,接收到消息 = " + textMessage.getText());
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }));
            System.in.read();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void consumerQueueTest2() {
        try {
            connection = connectFactory.createConnection();
            connection.start();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("ren-yan-queue-test");
            consumer = session.createConsumer(queue);
            consumer.setMessageListener((message -> {
                TextMessage textMessage = (TextMessage) message;
                try {
                    log.info("我是队列消费者2,接收到消息 = " + textMessage.getText());
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }));
            System.in.read();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {

        }
    }


    @Test
    public void consumerTopicTest1() {
        try {
            connection = connectFactory.createConnection();
            connection.start();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Topic topic1 = session.createTopic("ren-yan-topic-test");
            consumer = session.createConsumer(topic1);
            consumer.setMessageListener((message -> {
                TextMessage textMessage = (TextMessage) message;
                try {
                    log.info("我是主题消费者1,接收到消息 = " + textMessage.getText());
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

    @Test
    public void consumerTopicTest2() {
        try {
            connection = connectFactory.createConnection();
            connection.start();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Topic topic2 = session.createTopic("ren-yan-topic-test");
            consumer = session.createConsumer(topic2);
            consumer.setMessageListener((message -> {
                TextMessage textMessage = (TextMessage) message;
                try {
                    log.info("我是主题消费者2,接收到消息 = " + textMessage.getText());
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

    /**
     * 主题消费者 --- 持久化
     */
    public void consumerTopicPersistent() {
        try {
            log.info("我是张三");
            connection = connectFactory.createConnection();
            //connection.start();
            connection.setClientID("zhang-san");
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Topic topic2 = session.createTopic("ren-yan-topic-persistent");
            //主题订阅者
            TopicSubscriber subscriber = session.createDurableSubscriber(topic2, "remark");

            connection.start();
            Message message = subscriber.receive();
            while(null != message){
                TextMessage textMessage = (TextMessage)message;
                log.info("收到的持久化主题消息为 = " + textMessage.getText());
                //一直监听
                message = subscriber.receive();
            }
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
