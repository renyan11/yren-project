package com.fh.yren.activemq.consumer.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.stereotype.Service;

import javax.jms.JMSException;
import javax.jms.MapMessage;

@Service
@Slf4j
public class ActiveMqConsumerService {

    /**
     * 消费者消费消息
     *
     * @param mapMessage
     * @return
     */
    @JmsListener(destination = "ren-yan-20200212", containerFactory = "jmsQueueListener")
    public void consumerMessage(MapMessage mapMessage) throws JMSException {
        String username = mapMessage.getString("username");
        String age = mapMessage.getString("age");
        String gender = mapMessage.getString("gender");
        log.info("接受到的消息为：" + username + " " + age + " " + gender);
    }
}
