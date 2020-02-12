package com.fh.yren.activemq.config;

import com.fh.yren.activemq.util.ActiveMQUtil;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jms.config.DefaultJmsListenerContainerFactory;

import javax.jms.Session;

@Configuration
public class ActiveMqConfig {
    @Value("${spring.activemq.broker-url:disabled}")
    String brokerUrl;

    @Value("${activemq.listener.enable:disabled}")
    String listenerEnable;

    /**
     * ActivemqUtil工具类对象，并初始化创建连接
     *
     * @return
     */
    @Bean
    public ActiveMQUtil getActiveMQUtil() {
        if (brokerUrl.equals("disabled")) {
            return null;
        }
        ActiveMQUtil activeMQUtil = new ActiveMQUtil();
        activeMQUtil.init(brokerUrl);
        return activeMQUtil;
    }

    /**
     * ActiveMQ工厂对象
     *
     * @return
     */
    @Bean
    public ActiveMQConnectionFactory activeMQConnectionFactory() {
        return new ActiveMQConnectionFactory(brokerUrl);
    }

    /**
     * 定义一个消息连接器连接工厂
     *
     * @param activeMQConnectionFactory
     * @return
     */
    @Bean(name = "jmsQueueListener")
    public DefaultJmsListenerContainerFactory jmsListenerContainerFactory(ActiveMQConnectionFactory activeMQConnectionFactory) {
        DefaultJmsListenerContainerFactory defaultJmsListenerContainerFactory = new DefaultJmsListenerContainerFactory();
        if (!listenerEnable.equals("true")) {
            return null;
        }
        //设置工厂连接
        defaultJmsListenerContainerFactory.setConnectionFactory(activeMQConnectionFactory);
        //设置并发数
        defaultJmsListenerContainerFactory.setConcurrency("5");
        //设置恢复间隔为5s
        defaultJmsListenerContainerFactory.setRecoveryInterval(5000L);
        defaultJmsListenerContainerFactory.setSessionTransacted(false);
        //AUTO_ACKNOWLEDGE = 1    自动确认
        //CLIENT_ACKNOWLEDGE = 2    客户端手动确认
        //DUPS_OK_ACKNOWLEDGE = 3    自动批量确认
        //SESSION_TRANSACTED = 0    事务提交并确认
        defaultJmsListenerContainerFactory.setSessionAcknowledgeMode(Session.CLIENT_ACKNOWLEDGE);
        return defaultJmsListenerContainerFactory;
    }
}
