package com.fh.yren.activemq.provider.controller;

import com.fh.yren.activemq.provider.service.ActiveMqProviderService;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

@RestController
public class ActiveMqProviderController {


    @Resource
    ActiveMqProviderService activeMqProviderService;

    @RequestMapping("sendQueue")
    public String sendQueue(){
        return  activeMqProviderService.sendQueue();
    }
}
