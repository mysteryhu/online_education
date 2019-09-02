package com.atguigu.qz.service

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.online.util.JsonUtil
import com.atguigu.qz.bean._
import org.apache.spark.sql.{SaveMode, SparkSession}

object EtlDataService {

  /**
    * 解析章节数据
    * @param session
    */
  def etlQzChapter(session:SparkSession): Unit ={
    import session.implicits._
    session.sparkContext.textFile("/user/atguigu/ods/QzChapter.log")
      .filter(item=> {
          val jsonObject = JsonUtil.getJsonData(item)
          jsonObject.isInstanceOf[JSONObject]
      }).mapPartitions(partition=>
      partition.map(item=> {
        JSON.parseObject(item, classOf[QzChapter])
      }))
      .toDF()
      .coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_qz_chapter")
    session.sql("select * from dwd.dwd_qz_chapter").show()
  }

  /**
    * 解析章节列表数据
    * @param session
    */
  def etlQzChapterList(session:SparkSession): Unit ={
    import session.implicits._
    session.sparkContext.textFile("/user/atguigu/ods/QzChapterList.log")
      .filter(item=> {
        val jsonObject = JsonUtil.getJsonData(item)
        jsonObject.isInstanceOf[JSONObject]
      }).mapPartitions(partition=>
      partition.map(item=> {
        JSON.parseObject(item, classOf[QzChapterList])
      }))
      .toDF()
      .coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_qz_chapter_list")
    session.sql("select * from dwd.dwd_qz_chapter_list").show()
  }

  /**
    * 解析做题数据
    * 分数字段 decimal 转换
    * @param session
    */
  def etlQzPoint(session:SparkSession): Unit ={
    import session.implicits._
    session.sparkContext.textFile("/user/atguigu/ods/QzPoint.log")
      .filter(item=> {
        val jsonObject = JsonUtil.getJsonData(item)
        jsonObject.isInstanceOf[JSONObject]
      }).mapPartitions(partition=>
      partition.map(item=> {
        val point = JSON.parseObject(item, classOf[QzPoint])
        //分数字段保留一位小数,并四舍五入
        point.score=BigDecimal.apply(point.score).setScale(1,BigDecimal.RoundingMode.HALF_UP).toString()
        point
      }))
      .toDF()
      .coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_qz_point")
    session.sql("select * from dwd.dwd_qz_point").show()
  }

  /**
    * 做题知识点关联数据
    * @param session
    */
  def etlQzPointQuestion(session:SparkSession): Unit ={
    import session.implicits._
    session.sparkContext.textFile("/user/atguigu/ods/QzPointQuestion.log")
      .filter(item=> {
        val jsonObject = JsonUtil.getJsonData(item)
        jsonObject.isInstanceOf[JSONObject]
      }).mapPartitions(partition=>
      partition.map(item=> {
        JSON.parseObject(item, classOf[QzPointQuestion])
      }))
      .toDF()
      .coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_qz_point_question")
    session.sql("select * from dwd.dwd_qz_point_question").show()
  }

  /**
    * 解析网站课程
    * @param session
    */
  def etlQzSiteCourse(session:SparkSession): Unit ={
    import session.implicits._
    session.sparkContext.textFile("/user/atguigu/ods/QzSiteCourse.log")
      .filter(item=> {
        val jsonObject = JsonUtil.getJsonData(item)
        jsonObject.isInstanceOf[JSONObject]
      }).mapPartitions(partition=>
      partition.map(item=> {
        JSON.parseObject(item, classOf[QzSiteCourse])
      }))
      .toDF()
      .coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_qz_site_course")
    session.sql("select * from dwd.dwd_qz_site_course").show()
  }

  /**
    * 解析课程数据
    * @param session
    */
  def etlQzCourse(session:SparkSession): Unit ={
    import session.implicits._
    session.sparkContext.textFile("/user/atguigu/ods/QzCourse.log")
      .filter(item=> {
        val jsonObject = JsonUtil.getJsonData(item)
        jsonObject.isInstanceOf[JSONObject]
      }).mapPartitions(partition=>
      partition.map(item=> {
        JSON.parseObject(item, classOf[QzCourse])
      }))
      .toDF()
      .coalesce(1).write.mode(SaveMode.Overwrite)
      .insertInto("dwd.dwd_qz_course")
    session.sql("select * from dwd.dwd_qz_course").show()
  }



  /**
    * 解析课程辅导数据
    * @param session
    */
  def etlQzCourseEdusubject(session:SparkSession): Unit ={
    import session.implicits._
    session.sparkContext.textFile("/user/atguigu/ods/QzCourseEduSubject.log")
      .filter(item=> {
        val jsonObject = JsonUtil.getJsonData(item)
        jsonObject.isInstanceOf[JSONObject]
      }).mapPartitions(partition=>
      partition.map(item=> {
        JSON.parseObject(item, classOf[QzCourseEduSubject])
      }))
      .toDF()
      .coalesce(1).write.mode(SaveMode.Overwrite)
      .insertInto("dwd.dwd_qz_course_edusubject")
    session.sql("select * from dwd.dwd_qz_course_edusubject").show()
  }

  /**
    * 解析课程网站
    * @param session
    */
  def etlQzWebsite(session:SparkSession): Unit ={
    import session.implicits._
    session.sparkContext.textFile("/user/atguigu/ods/QzWebsite.log")
      .filter(item=> {
        val jsonObject = JsonUtil.getJsonData(item)
        jsonObject.isInstanceOf[JSONObject]
      }).mapPartitions(partition=>
      partition.map(item=> {
        JSON.parseObject(item, classOf[QzWebsite])
      }))
      .toDF()
      .coalesce(1).write.mode(SaveMode.Overwrite)
      .insertInto("dwd.dwd_qz_website")
    session.sql("select * from dwd.dwd_qz_website").show()
  }


  /**
    * 解析主修数据
    * @param session
    */
  def etlQzMajor(session:SparkSession): Unit ={
    import session.implicits._
    session.sparkContext.textFile("/user/atguigu/ods/QzMajor.log")
      .filter(item=> {
        val jsonObject = JsonUtil.getJsonData(item)
        jsonObject.isInstanceOf[JSONObject]
      }).mapPartitions(partition=>
      partition.map(item=> {
        JSON.parseObject(item, classOf[QzMajor])
      }))
      .toDF()
      .coalesce(1).write.mode(SaveMode.Overwrite)
      .insertInto("dwd.dwd_qz_major")
    session.sql("select * from dwd.dwd_qz_major").show()
  }

  /**
    * 解析做题业务
    * @param session
    */
  def etlQzBusiness(session:SparkSession): Unit ={
    import session.implicits._
    session.sparkContext.textFile("/user/atguigu/ods/QzBusiness.log")
      .filter(item=> {
        val jsonObject = JsonUtil.getJsonData(item)
        jsonObject.isInstanceOf[JSONObject]
      }).mapPartitions(partition=>
      partition.map(item=> {
        JSON.parseObject(item, classOf[QzBusiness])
      }))
      .toDF()
      .coalesce(1).write.mode(SaveMode.Overwrite)
      .insertInto("dwd.dwd_qz_business")
    session.sql("select * from dwd.dwd_qz_business").show()
  }


  /**
    * 解析试卷视图数据
    * @param session
    */
  def etlQzPaperView(session:SparkSession): Unit ={
    import session.implicits._
    session.sparkContext.textFile("/user/atguigu/ods/QzPaperView.log")
      .filter(item=> {
        val jsonObject = JsonUtil.getJsonData(item)
        jsonObject.isInstanceOf[JSONObject]
      }).mapPartitions(partition=>
      partition.map(item=> {
        JSON.parseObject(item, classOf[QzPaperView])
      }))
      .toDF()
      .coalesce(1).write.mode(SaveMode.Overwrite)
      .insertInto("dwd.dwd_qz_paper_view")
    session.sql("select * from dwd.dwd_qz_paper_view").show()
  }

  /**
    *
    * @param session
    */
  def etlQzCenterPaper(session:SparkSession): Unit ={
    import session.implicits._
    session.sparkContext.textFile("/user/atguigu/ods/QzCenterPaper.log")
      .filter(item=> {
        val jsonObject = JsonUtil.getJsonData(item)
        jsonObject.isInstanceOf[JSONObject]
      }).mapPartitions(partition=>
      partition.map(item=> {
        JSON.parseObject(item, classOf[QzCenterPaper])
      }))
      .toDF()
      .coalesce(1).write.mode(SaveMode.Overwrite)
      .insertInto("dwd.dwd_qz_center_paper")
    session.sql("select * from dwd.dwd_qz_center_paper").show()
  }

  /**
    * 分数字段需要转换
    * @param session
    */
  def etlQzPaper(session:SparkSession): Unit ={
    import session.implicits._
    session.sparkContext.textFile("/user/atguigu/ods/QzPaper.log")
      .filter(item=> {
        val jsonObject = JsonUtil.getJsonData(item)
        jsonObject.isInstanceOf[JSONObject]
      }).mapPartitions(partition=>
      partition.map(item=> {
        val paper: QzPaper = JSON.parseObject(item, classOf[QzPaper])
        paper.totalscore = BigDecimal.apply(paper.totalscore).setScale(1,BigDecimal.RoundingMode.HALF_UP).toString()
        paper
      }))
      .toDF()
      .coalesce(1).write.mode(SaveMode.Overwrite)
      .insertInto("dwd.dwd_qz_paper")
    session.sql("select * from dwd.dwd_qz_paper").show()
  }

  /**
    * 解析主题数据
    * @param session
    */
  def etlQzCenter(session:SparkSession): Unit ={
    import session.implicits._
    session.sparkContext.textFile("/user/atguigu/ods/QzCenter.log")
      .filter(item=> {
        val jsonObject = JsonUtil.getJsonData(item)
        jsonObject.isInstanceOf[JSONObject]
      }).mapPartitions(partition=>
      partition.map(item=> {
        JSON.parseObject(item, classOf[QzCenter])
      }))
      .toDF()
      .coalesce(1).write.mode(SaveMode.Overwrite)
      .insertInto("dwd.dwd_qz_center")
    session.sql("select * from dwd.dwd_qz_center").show()
  }

  /**
    * 分数字段需要精度转换
    * @param session
    */
  def etlQzQuestion(session:SparkSession): Unit ={
    import session.implicits._
    session.sparkContext.textFile("/user/atguigu/ods/QzQuestion.log")
      .filter(item=> {
        val jsonObject = JsonUtil.getJsonData(item)
        jsonObject.isInstanceOf[JSONObject]
      }).mapPartitions(partition=>
      partition.map(item=> {
        val question: QzQuestion = JSON.parseObject(item, classOf[QzQuestion])
        question.score = BigDecimal.apply(question.score).setScale(1,BigDecimal.RoundingMode.HALF_UP).toString()
        question.splitscore = BigDecimal.apply(question.splitscore).setScale(1,BigDecimal.RoundingMode.HALF_UP).toString()
        question
      }))
      .toDF()
      .coalesce(1).write.mode(SaveMode.Overwrite)
      .insertInto("dwd.dwd_qz_question")
    session.sql("select * from dwd.dwd_qz_question").show()
  }

  /**
    *
    * @param session
    */
  def etlQzQuestionType(session:SparkSession): Unit ={
    import session.implicits._
    session.sparkContext.textFile("/user/atguigu/ods/QzQuestionType.log")
      .filter(item=> {
        val jsonObject = JsonUtil.getJsonData(item)
        jsonObject.isInstanceOf[JSONObject]
      }).mapPartitions(partition=>
      partition.map(item=> {
        JSON.parseObject(item, classOf[QzQuestionType])
      }))
      .toDF()
      .coalesce(1).write.mode(SaveMode.Overwrite)
      .insertInto("dwd.dwd_qz_question_type")
    session.sql("select * from dwd.dwd_qz_question_type").show()
  }



  /**
    *解析用户做题情况数据
    * 分数字段需要转换精度
    * @param session
    */
  def etlQzMemberPaperQuestion(session:SparkSession): Unit ={
    import session.implicits._
    session.sparkContext.textFile("/user/atguigu/ods/QzMemberPaperQuestion.log")
      .filter(item=> {
        val jsonObject = JsonUtil.getJsonData(item)
        jsonObject.isInstanceOf[JSONObject]
      }).mapPartitions(partition=>
      partition.map(item=> {
        val question: QzMemberPaperQuestion = JSON.parseObject(item, classOf[QzMemberPaperQuestion])
        question.score = BigDecimal.apply(question.score).setScale(1,BigDecimal.RoundingMode.HALF_UP).toString()
        question
      }))
      .toDF()
      .coalesce(1).write.mode(SaveMode.Overwrite)
      .insertInto("dwd.dwd_qz_member_paper_question")
    session.sql("select * from dwd.dwd_qz_member_paper_question").show()
  }



}
