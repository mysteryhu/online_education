package com.atguigu.online_ads1

object DemandTable {

  //各注册跳转地址(appregurl)进行注册的用户数
  def demand4(dt:String): String ={
    val sql = s"select appregurl,count(uid),dn,dt from dws.dws_member where dt = '${dt}' group by appregurl,dn,dt"
    sql
  }

  //5.统计各所属网站（sitename）的用户数
  def demand5(dt:String): String ={
    val sql = s"select sitename,count(uid),dn,dt from dws.dws_member where dt = '${dt}' group by sitename,dn,dt"
    sql
  }

  //6.统计各所属平台的（regsourcename）用户数
  def demand6(dt:String): String ={
    val sql = s"select regsourcename,count(uid),dn,dt from dws.dws_member where dt= '${dt}' group by regsourcename,dn,dt"
    sql
  }

  //7.统计通过各广告跳转（adname）的用户数
  def demand7(dt:String): String ={
    val sql =s"select adname,count(uid),dn,dt from dws.dws_member where dt ='${dt}' group by adname,dn,dt"
    sql
  }

  //8.需求 使用Spark DataFrame Api统计各用户级别（memberlevel）的用户数
  def demand8(dt:String): String = {
    val sql =s"select memberlevel,count(uid),dn,dt from dws.dws_member where dt ='${dt}' group by memberlevel,dn,dt"
    sql
  }
  //需求9：使用Spark DataFrame Api统计各vip等级人数
  def demand9(dt:String): String = {
    val sql =s"select vip_level,count(uid),dn,dt from dws.dws_member where dt ='${dt}' group by vip_level,dn,dt"
    sql
  }

  //需求10：使用Spark DataFrame Api统计各分区网站、用户级别下(website、memberlevel)的top3用户
  def demand10(dt:String): String = {
    val sql =s"select * from (select uid,ad_id,memberlevel,register,appregurl,regsource,regsourcename,adname," +
      s"siteid,sitename,vip_level,cast(paymoney as decimal(10,4)),row_number() over(partition by memberlevel order by paymoney desc) rk," +
      s"dn from dws.dws_member where dt = '${dt}')r where rk<4 order by memberlevel,rk"
    sql
  }
}
