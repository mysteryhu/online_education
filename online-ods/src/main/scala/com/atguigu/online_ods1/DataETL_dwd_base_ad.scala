package com.atguigu.online_ods1

import com.alibaba.fastjson.JSON
import com.atguigu.online_bean.dwd_base_ad
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}


object DataETL_dwd_base_ad {

  def main(args: Array[String]): Unit = {

    //创建sparkconf
    val conf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName)

    //创建sparksession
    val spark = SparkSession.builder()
      .appName("Spark Hive ODS")
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()

    //hiveContext
    //val hiveContext = new HiveContext()
    // TODO 读取数据源 dwd_base_ad
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

//    spark.sql("set hive.mapred.supports.subdirectories=true")
//    spark.sql("set mapreduce.input.fileinputformat.input.dir.recursive=true")
//    spark.sql("set mapred.max.split.size=256000000")
//    spark.sql("set mapred.min.split.size.per.node=128000000")
//    spark.sql("set mapred.min.split.size.per.rack=128000000")
//    spark.sql("set hive.hadoop.supports.splittable.combineinputformat=true")
//    spark.sql("set hive.exec.compress.output=true")
//    spark.sql("set mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec")
//    spark.sql("set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat")
//
//    spark.sql("set hive.merge.mapfiles=true")
//    spark.sql("set hive.merge.mapredfiles=true")
//    spark.sql("set hive.merge.size.per.task=256000000")
//    spark.sql("set hive.merge.smallfiles.avgsize=256000000")
//
//    spark.sql("set hive.groupby.skewindata=true")

//    spark.sql("set hive.exec.reducers.bytes.per.reducer=500000000")
//    spark.sql("set hive.mapred.supports.subdirectories=true")
//    spark.sql("set mapreduce.input.fileinputformat.input.dir.recursive=true")
//    spark.sql("set mapred.max.split.size=256000000")
//    spark.sql("set mapred.min.split.size.per.node=128000000")
//    spark.sql("set mapred.min.split.size.per.rack=128000000")
//    spark.sql("set hive.hadoop.supports.splittable.combineinputformat=true")
//    spark.sql("set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat")
//    spark.sql("set hive.merge.mapfiles=true")
//    spark.sql("set hive.merge.mapredfiles=true")
//    spark.sql("set hive.merge.size.per.task=256000000")
//    spark.sql("set hive.merge.smallfiles.avgsize=256000000")
//    spark.sql("set hive.groupby.skewindata=true")
//    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
//    spark.sql("set hive.exec.parallel=true")
//    spark.sql("set hive.exec.parallel.thread.number=32")
//    spark.sql("SET hive.exec.compress.output=true")
//    spark.sql("SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec")
//    spark.sql("SET mapred.output.compression.type=BLOCK")
//    spark.sql("set hive.exec.compress.intermediate=true")
//    spark.sql("set hive.intermediate.compression.codec=org.apache.hadoop.io.compress.SnappyCodec")
//    spark.sql("set hive.intermediate.compression.type=BLOCK")


    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    spark.sql("insert overwrite table dwd.dwd_base_ad select * from dwd_base_ad")
    spark.sql("select * from dwd.dwd_base_ad").show()

    //创建数据



  }
}
