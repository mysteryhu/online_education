package com.atguigu.online_ods1

import com.alibaba.fastjson.JSON
import com.atguigu.online_bean.dwd_base_website
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object DataETL_dwd_base_website {

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
    val source: RDD[String] = spark.sparkContext.textFile("/user/atguigu/ods/baswewebsite.log")
    import spark.implicits._

    //脱敏 &&  数据清洗
    val mapRDD = source.map(rdd => {
      JSON.parseObject(rdd, classOf[dwd_base_website])
    }).toDF()
      .createOrReplaceTempView("dwd_base_website")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    spark.sql("insert overwrite table dwd.dwd_base_website select * from dwd_base_website")
    //spark.sql("select * from dwd.dwd_base_ad").show()

  }
}
