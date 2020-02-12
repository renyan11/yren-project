package com.fh.yren.activemq.util;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.pool.PooledConnectionFactory;
import javax.jms.ConnectionFactory;

public class ActiveMQUtil {

    PooledConnectionFactory pooledConnectionFactory = null;

    public ConnectionFactory init(String brokerUrl){
        ActiveMQConnectionFactory activeMQConnectionFactory = new ActiveMQConnectionFactory(brokerUrl);
        //加入连接池
        pooledConnectionFactory = new PooledConnectionFactory(activeMQConnectionFactory);
        //出现异常时重新连接
        pooledConnectionFactory.setReconnectOnException(true);
        //最大连接数
        pooledConnectionFactory.setMaxConnections(10);
        //超时时间
        pooledConnectionFactory.setExpiryTimeout(30000);
        return pooledConnectionFactory;
    }

    public ConnectionFactory getConnectFactory(){
        return pooledConnectionFactory;
    }
}
