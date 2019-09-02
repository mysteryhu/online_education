package com.atguigu.util

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object SCUtil {

  def getSC():SparkSession  ={

    val conf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName)
      conf.set("spark.debug.maxToStringFields","10000")
    //创建sparksession
    //设置kryo序列化
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //conf.registerKryoClasses(Array(classOf[想要kryo的bean]))
    //需要更改缓存级别
//    rdd.persist(StorageLevel.MEMORY_AND_DISK_SER)
    //关于kryo:
    //1.rdd需要注册实现kryo序列化
    //2.DF,DS 实现了kryo序列化,默认kryo序列化
    val spark = SparkSession.builder()
      .appName("Spark Hive ODS")
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()

    spark
  }

}
