package com.atguigu.online.util;

import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;

/**
 * @Date 2019/8/31
 * @Version JDK 1.8
 **/
public class JsonUtil {
    /**
     * 判断是否为json字符串,如果是真,返回JSONObject
     * 如果不符合json规则,抛出异常,返回null
     * @param data json字符串
     * @return
     */
    public static JSONObject getJsonData(String data){
        try{
             return JSONObject.parseObject(data);
        }catch (JSONException e){
            return null;
        }
    }
}
