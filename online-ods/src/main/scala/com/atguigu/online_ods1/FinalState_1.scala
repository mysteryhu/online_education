package com.atguigu.online_ods1

import com.alibaba.fastjson.JSON
import com.atguigu.online_bean._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object FinalState_1 {
  def main(args: Array[String]): Unit = {

    //创建sparkconf
    val conf = new SparkConf().setMaster("yarn-cluster").setAppName(this.getClass.getSimpleName)

    //创建sparksession
    val spark = SparkSession.builder()
      .appName("Spark Hive ODS")
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()

    //hiveContext
    //val hiveContext = new HiveContext()

    // TODO 1 读取数据源 dwd_base_ad
    val baseadlog:String = "/user/atguigu/ods/baseadlog.log"
    val rdd: RDD[String] = spark.sparkContext.textFile(baseadlog)

    //脱敏
    val base: RDD[dwd_base_ad] = rdd.filter(!_.isEmpty)
      .map { line => {
        JSON.parseObject(line, classOf[dwd_base_ad])
      }
      }

    //RDD转成DF
    import spark.implicits._

    val adDF: DataFrame = base.map { x =>
      (x.adid, x.adname, x.dn)
    }.toDF()

    adDF.createOrReplaceTempView("dwd_base_ad")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    spark.sql("insert overwrite table dwd.dwd_base_ad select * from dwd_base_ad")

    //TODO 2读取hdfs数据源 dwd_base_website
    val source: RDD[String] = spark.sparkContext.textFile("/user/atguigu/ods/baswewebsite.log")
    import spark.implicits._

    //脱敏 &&  数据清洗
    val mapRDD = source.map(rdd => {
      JSON.parseObject(rdd, classOf[dwd_base_website])
    }).toDF()
      .createOrReplaceTempView("dwd_base_website")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    spark.sql("insert overwrite table dwd.dwd_base_website select * from dwd_base_website")


    //TODO 3 读取hdfs数据源 dwd_member
    val source3: RDD[String] = spark.sparkContext.textFile("/user/atguigu/ods/member.log")//
    import spark.implicits._

    //脱敏 &&  数据清洗
    source3.map(rdd => {
      var member: dwd_member = JSON.parseObject(rdd, classOf[dwd_member]) //
      member.fullname=member.fullname.substring(0,1)+"XX"
      member.phone=member.phone.substring(0,3)+"*****"+member.phone.substring(8,11)//
      member.password = "******"
      member
    }).toDF()
      .createOrReplaceTempView("dwd_member") //

    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    spark.sql("insert overwrite table dwd.dwd_member select * from dwd_member")

    //TODO 4 读取hdfs数据源 dwd_member_regtype
    val source4: RDD[String] = spark.sparkContext.textFile("/user/atguigu/ods/memberRegtype.log")
    import spark.implicits._

    //脱敏 &&  数据清洗
    source4.map(rdd => {
      JSON.parseObject(rdd, classOf[dwd_member_regtype]) //
    }).toDF()
      .createOrReplaceTempView("dwd_member_regtype") //


    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    spark.sql("insert overwrite table dwd.dwd_member_regtype select * from dwd_member_regtype")

    //TODO 5 读取hdfs数据源 dwd_pcentermempaymoney
    val source5: RDD[String] = spark.sparkContext.textFile("/user/atguigu/ods/pcentermempaymoney.log")
    import spark.implicits._

    //脱敏 &&  数据清洗
   source5.map(rdd => {
      JSON.parseObject(rdd, classOf[dwd_pcentermempaymoney])//
    }).toDF()
      .createOrReplaceTempView("dwd_pcentermempaymoney")//


    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    spark.sql("insert overwrite table dwd.dwd_pcentermempaymoney select * from dwd_pcentermempaymoney")

    //TODO 读取hdfs数据源 dwd_vip_level
    val source6: RDD[String] = spark.sparkContext.textFile("/user/atguigu/ods/pcenterMemViplevel.log")//
    import spark.implicits._

    //脱敏 &&  数据清洗
    source6.map(rdd => {
      JSON.parseObject(rdd, classOf[dwd_vip_level])//
      //
    }).toDF()
      .createOrReplaceTempView("dwd_vip_level")//


    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    spark.sql("insert overwrite table dwd.dwd_vip_level select * from dwd_vip_level")
  }
}
