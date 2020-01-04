package com.atguigu.gmall.dw.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.dw.service.PublisherService;
import org.apache.commons.lang.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@RestController
public class PublisherController {
    @Autowired
    PublisherService publisherService;

    @GetMapping("realtime-total")
    public String realtimeHourDate(@RequestParam("date") String date) {
        List<Map> list = new ArrayList<Map>();
        // 日活总数
        int dauTotal = publisherService.getDauTotal(date);
        Map dauMap = new HashMap<String, Object>();
        dauMap.put("id", "dau");
        dauMap.put("name", "新增日活");
        dauMap.put("value", dauTotal);
        list.add(dauMap);

        /*// 新增用户
        int newMidTotal = publisherService.getNewMidTotal(date);
        Map newMidMap = new HashMap<String, Object>();
        newMidMap.put("id", "new_mid");
        newMidMap.put("name", "新增用户");
        newMidMap.put("value", newMidTotal);
        list.add(newMidMap);*/

        return JSON.toJSONString(list);
    }


    @GetMapping("realtime-hours")
    public String realtimeHourDate(@RequestParam("id") String id, @RequestParam("date") String date) {

        if ("dau".equals(id)) {
            Map dauHoursToday = publisherService.getDauHours(date);
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("today", dauHoursToday);
            String yesterdayDateString = "";
            try {
                Date dateToday = new SimpleDateFormat("yyyy-MM-dd").parse(date);
                Date dateYesterday = DateUtils.addDays(dateToday, -1);
                yesterdayDateString = new SimpleDateFormat("yyyy-MM-dd").format(dateYesterday);

            } catch (ParseException e) {
                e.printStackTrace();
            }
            Map dauHoursYesterday = publisherService.getDauHours(yesterdayDateString);
            jsonObject.put("yesterday", dauHoursYesterday);
            return jsonObject.toJSONString();
        }


        /*if ("new_order_totalamount".equals(id)) {
            String newOrderTotalamountJson = publisherService.getNewOrderTotalAmountHours(date);
            return newOrderTotalamountJson;
        }*/
        return null;
    }

}
