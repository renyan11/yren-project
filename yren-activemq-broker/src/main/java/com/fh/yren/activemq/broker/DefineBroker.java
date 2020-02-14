package com.fh.yren.activemq.broker;

import org.apache.activemq.broker.BrokerService;

public class DefineBroker {

    public static void main(String[] args) {
        BrokerService brokerService = new BrokerService();
        try{
            brokerService.setUseJmx(true);
            brokerService.addConnector("tcp://localhost:61616");
            brokerService.start();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
