package com.atguigu.online_ods1

import com.alibaba.fastjson.JSON
import com.atguigu.online_bean.{dwd_member_regtype, dwd_pcentermempaymoney}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object DataETL_dwd_member_regtype {
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
    val source: RDD[String] = spark.sparkContext.textFile("/user/atguigu/ods/memberRegtype.log")
    import spark.implicits._

    //脱敏 &&  数据清洗
    val mapRDD = source.map(rdd => {
      var dwd_member_regtype = JSON.parseObject(rdd, classOf[dwd_member_regtype])
      dwd_member_regtype.regsource match {
        case "1" => dwd_member_regtype.regsourcename="PC"
        case "2" => dwd_member_regtype.regsourcename="MOBLINE"
        case "3" => dwd_member_regtype.regsourcename="APP"
        case "4" => dwd_member_regtype.regsourcename="WECHAT"
        case _ => dwd_member_regtype.regsourcename=""
      }
      dwd_member_regtype
    }).toDF()
      .createOrReplaceTempView("dwd_member_regtype") //


    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    spark.sql("insert overwrite table dwd.dwd_member_regtype select * from dwd_member_regtype").coalesce(1) //
    spark.sql("select * from dwd.dwd_member_regtype").show()
  }
}
