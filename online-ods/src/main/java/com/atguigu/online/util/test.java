package com.atguigu.online.util;

import com.alibaba.fastjson.JSONObject;

/**
 * @Date 2019/9/1
 * @Version JDK 1.8
 **/
public class test {
    public static void main(String[] args) {
        JSONObject object = JSONObject.parseObject("{\"chapterid\":99999,\"chapterlistid\":99999,\"chaptername\":\"chaptername99999\",\"chapternum\":10,\"courseid\":59,\"createtime\":\"2019-07-22 16:37:25\",\"creator\":\"admin\",\"dn\":\"webA\",\"dt\":\"20190722\",\"outchapterid\":0,\"sequence\":\"-\",\"showstatus\":\"-\",\"status\":\"-\"}");
        JSONObject object1 = JSONObject.parseObject("aa,bb");
        System.out.println(object1);
        System.out.println(object);
    }
}
