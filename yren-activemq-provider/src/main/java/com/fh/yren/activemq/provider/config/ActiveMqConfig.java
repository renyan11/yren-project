package com.fh.yren.activemq.provider.config;

import com.fh.yren.activemq.provider.util.ActiveMQUtil;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ActiveMqConfig {
    @Value("${spring.activemq.broker-url:disabled}")
    String brokerUrl;

    @Value("${activemq.listener.enable:disabled}")
    String listenerEnable;

    @Bean
    public ActiveMQUtil getActiveMQUtil(){
        if(brokerUrl.equals("disabled")){
            return null;
        }
        ActiveMQUtil activeMQUtil = new ActiveMQUtil();
        activeMQUtil.init(brokerUrl);
        return activeMQUtil;
    }

    @Bean
    public ActiveMQConnectionFactory activeMQConnectionFactory(){
        return new ActiveMQConnectionFactory(brokerUrl);
    }
}
