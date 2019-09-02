package com.atguigu.online_dws1

import java.text.SimpleDateFormat

import com.atguigu.online_bean.{MemberZipperList, dws_member_zipper}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

object DWS_Member {

      //隐式转换
    //TODO  宽表
    def insertMember(ssc: SparkSession,date: String) {
      import ssc.implicits._
      import org.apache.spark.sql.functions._
      ssc.sql("set hive.exec.dynamic.partition.mode=nonstrict")
      val rdd = ssc.sql("select uid,first(ad_id),first(fullname),first(iconurl),first(lastlogin)," +
        "first(mailaddr),first(memberlevel),first(password),sum(cast(paymoney as decimal(10,4))),first(phone),first(qq)," +
        "first(register),first(regupdatetime),first(unitname),first(userip),first(zipcode)," +
        "first(appkey),first(appregurl),first(bdp_uuid),first(reg_createtime),first(domain)," +
        "first(isranreg),first(regsource),first(regsourcename),first(adname),first(siteid),first(sitename)," +
        "first(siteurl),first(site_delete),first(site_createtime),first(site_creator),first(vip_id),max(vip_level)," +
        "min(vip_start_time),max(vip_end_time),max(vip_last_modify_time),first(vip_max_free),first(vip_min_free),max(vip_next_level)," +
        "first(vip_operator),dt,dn from" +
        "(select a.uid,a.ad_id,a.fullname,a.iconurl,a.lastlogin,a.mailaddr,a.memberlevel," +
        "a.password,e.paymoney,a.phone,a.qq,a.register,a.regupdatetime,a.unitname,a.userip," +
        "a.zipcode,a.dt,b.appkey,b.appregurl,b.bdp_uuid,b.createtime as reg_createtime,b.domain,b.isranreg,b.regsource," +
        "b.regsourcename,c.adname,d.siteid,d.sitename,d.siteurl,d.delete as site_delete,d.createtime as site_createtime," +
        "d.creator as site_creator,f.vip_id,f.vip_level,f.start_time as vip_start_time,f.end_time as vip_end_time," +
        "f.last_modify_time as vip_last_modify_time,f.max_free as vip_max_free,f.min_free as vip_min_free," +
        "f.next_level as vip_next_level,f.operator as vip_operator,a.dn " +
        s"from dwd.dwd_member a left join dwd.dwd_member_regtype b on a.uid=b.uid " +
        "and a.dn=b.dn left join dwd.dwd_base_ad c on a.ad_id=c.adid and a.dn=c.dn left join " +
        " dwd.dwd_base_website d on b.websiteid=d.siteid and b.dn=d.dn left join dwd.dwd_pcentermempaymoney e" +
        s" on a.uid=e.uid and a.dn=e.dn left join dwd.dwd_vip_level f on e.vip_id=f.vip_id and e.dn=f.dn where a.dt='${date}')r  " +
        "group by uid,dn,dt")
//        .select("uid").rdd.cache()
//      rdd.foreach(item=>item)
//      while (true){
//        println("1")
//      }
    .coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dws.dws_member")

  //查询当天增量
  val new_zipper: Dataset[dws_member_zipper] = ssc.sql(s"select uid,sum(cast(paymoney as decimal(10,4))) as paymoney," +
    s"max(vip_level) vip_level,from_unixtime(unix_timestamp(a.dt,'yyyyMMdd'),'yyyy-MM-dd') as start_time," +
    s"'9999-12-31' as end_time,a.dn dn from dwd.dwd_pcentermempaymoney a join dwd.dwd_vip_level b on" +
    s" a.vip_id = b.vip_id and a.dn = b.dn where a.dt  = '${date}' group by uid,a.dn,dt").as[dws_member_zipper]
  //查询拉链表旧数据
  val old_zipper: Dataset[dws_member_zipper] = ssc.sql("select * from dws.dws_member_zipper").as[dws_member_zipper]
  new_zipper.union(old_zipper).groupByKey(a=>a.uid+"_"+a.dn)
    .mapGroups { case (key, itr) =>
      val str = key.split("_")
      val uid =str(0)
      val dn =str(1)
      val sortItr: List[dws_member_zipper] = itr.toList.sortBy(_.start_time)
      if(sortItr.length>1 && "9999-12-31".equals(sortItr(sortItr.length-2).end_time)){
        //说明有新数据来了
        var old_data = sortItr(sortItr.length-2)
        var new_data = sortItr(sortItr.length-1)
        //新数据的endtime变为9999
        new_data.end_time = old_data.end_time
        //老数据的endtime 变为新数据的开始
        old_data.end_time = new_data.start_time
        //更新新数据的payment
        new_data.paymoney=(BigDecimal.apply(old_data.paymoney) + BigDecimal.apply(new_data.paymoney)).toString()
      }
      MemberZipperList(sortItr)
    }.flatMap(_.list).coalesce(3).write.mode(SaveMode.Overwrite).insertInto("dws.dws_member_zipper")
    }

}
