package com.atguigu.gmall.dw.service.imp;

import com.atguigu.gmall.dw.mapper.DauMapper;
import com.atguigu.gmall.dw.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PublisherServiceImpl implements PublisherService {
    @Autowired
    DauMapper dauMapper;

    @Override
    public int getDauTotal(String date) {
        return dauMapper.selectDauTotal(date);
    }

    @Override
    public Map getDauHours(String date) {
        HashMap dauHourMap = new HashMap();
        List<Map> dauHourList = dauMapper.selectDauTotalHourMap(date);
        for (Map map : dauHourList) {
            dauHourMap.put(map.get("LH"), map.get("CT"));
        }

        return dauHourMap;
    }


}
