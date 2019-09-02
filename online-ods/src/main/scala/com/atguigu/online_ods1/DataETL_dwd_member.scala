package com.atguigu.online_ods1

import com.alibaba.fastjson.JSON
import com.atguigu.online_bean.{dwd_member, dwd_member_regtype}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object DataETL_dwd_member {
  def main(args: Array[String]): Unit = {

    //创建sparkconf
    val conf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName)
    //创建sparksession
    val spark = SparkSession.builder()
      .appName("Spark Hive ODS")
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()

    //读取hdfs数据源
    val source: RDD[String] = spark.sparkContext.textFile("/user/atguigu/ods/member.log")//
    import spark.implicits._

    //脱敏 &&  数据清洗
    source.map(rdd => {
      var member: dwd_member = JSON.parseObject(rdd, classOf[dwd_member]) //
      member.fullname=member.fullname.substring(0,1)+"XX"
      member.phone=member.phone.substring(0,3)+"*****"+member.phone.substring(8,11)//
      member.password = "******"
      member
    }).toDF()
      .createOrReplaceTempView("dwd_member") //


    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    spark.sql("insert overwrite table dwd.dwd_member select * from dwd_member") //
    //spark.sql("select * from dwd.dwd_member").show()
  }
}
