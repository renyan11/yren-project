package com.fh.yren.activemq.provider.controller;

import com.fh.yren.activemq.util.ActiveMQUtil;
import org.apache.activemq.command.ActiveMQMapMessage;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import javax.jms.*;

@RestController
public class ActiveMqProviderController {

    @Resource
    ActiveMQUtil activeMQUtil;

    @RequestMapping("sendQueue")
    public String sendQueue(){
        ConnectionFactory connectFactory = activeMQUtil.getConnectFactory();
        Connection connection = null;
        Session session = null;
        MessageProducer producer = null;
        try{
            connection = connectFactory.createConnection();
            connection.start();
            session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue("ren-yan-20200212");
            ActiveMQMapMessage activeMQMapMessage = new ActiveMQMapMessage();
            activeMQMapMessage.setString("username","ren-yan");
            activeMQMapMessage.setInt("age",28);
            activeMQMapMessage.setString("gender","female");
            producer = session.createProducer(queue);
            producer.send(activeMQMapMessage);
            session.commit();
            return "send success";
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if(producer != null){
                try {
                    producer.close();
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
            if(session != null){
                try {
                    session.close();
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
            if(connection != null){
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
