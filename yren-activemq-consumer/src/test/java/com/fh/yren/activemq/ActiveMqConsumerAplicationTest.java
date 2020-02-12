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
        aplicationTest.consumerTest1();
        aplicationTest.consumerTest2();
    }

    @Test
    public void consumerTest1() {
        try {
            connection = connectFactory.createConnection();
            connection.start();
            session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("ren-yan-queue-test");
            consumer = session.createConsumer(queue);
            consumer.setMessageListener((message -> {
                TextMessage textMessage = (TextMessage) message;
                try {
                    log.info("我是消费者1,接收到消息 = " + textMessage.getText());
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void consumerTest2() {
        try {
            connection = connectFactory.createConnection();
            connection.start();
            session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("ren-yan-queue-test");
            consumer = session.createConsumer(queue);
            consumer.setMessageListener((message -> {
                TextMessage textMessage = (TextMessage) message;
                try {
                    log.info("我是消费者2,接收到消息 = " + textMessage.getText());
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {

        }
    }

}
