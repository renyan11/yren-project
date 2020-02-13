package com.fh.yren.activemq.provider.service;

import com.fh.yren.activemq.util.ActiveMQUtil;
import org.apache.activemq.command.ActiveMQMapMessage;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.jms.*;

@Service
public class ActiveMqProviderService {

    @Resource
    ActiveMQUtil activeMQUtil;


    public String sendQueue() {
        ConnectionFactory connectFactory = activeMQUtil.getConnectFactory();
        Connection connection = null;
        Session session = null;
        MessageProducer producer = null;
        try {
            connection = connectFactory.createConnection();
            connection.start();
            session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("ren-yan-20200212");
            ActiveMQMapMessage activeMQMapMessage = new ActiveMQMapMessage();
            activeMQMapMessage.setString("username", "ren-yan-123");
            activeMQMapMessage.setInt("age", 28);
            activeMQMapMessage.setString("gender", "male");
            producer = session.createProducer(queue);
            producer.send(activeMQMapMessage);
            session.commit();
            return "send success";
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
        return "send failure";
    }
}
