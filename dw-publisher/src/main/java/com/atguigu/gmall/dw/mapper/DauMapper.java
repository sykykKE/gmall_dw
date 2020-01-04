package com.atguigu.gmall.dw.mapper;

import java.util.*;

public interface DauMapper {
    public Integer selectDauTotal(String date);

    public List<Map> selectDauTotalHourMap(String date);
}
