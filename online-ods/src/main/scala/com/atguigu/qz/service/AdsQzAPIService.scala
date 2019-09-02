package com.atguigu.qz.service


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
//TODO SparkSQL_API实现
//报表层各指标统计
object AdsQzAPIService {



  /**
    * 统计各试卷平均分,平均耗时
    * @param session
    * @param dt
    * @return
    */
    def saveAdsPaperPassDetailAPI(session:SparkSession,dt:String)={
      session.sql("select paperviewid,paperviewname,score,spendtime,dt,dn from dws.dws_user_paper_detail")
        .where(s"dt=${dt}")
        .groupBy("paperviewid", "paperviewname", "dt", "dn")
        .agg(avg("score").cast("decimal(4,1)").as("avgscore"),
          avg("spendtime").cast("decimal(10,2)").as("avgspendtime"))
        .orderBy(desc("avgscore"),desc("avgspendtime"))
        .show()
    }

  /**
    * 统计各试卷最高分、最低分
    * @param session
    * @param dt
    */
  def saveTopScoreAPI(session:SparkSession,dt:String)={
    session.sql(s"select paperviewid,paperviewname,score,dt,dn from  dws.dws_user_paper_detail")
      .where(s"dt=${dt}")
      .groupBy("paperviewid","paperviewname","dt","dn")
      .agg(max("score").cast("decimal(4,1)").as("maxscore")
      ,min("score").cast("decimal(4,1)").as("minscore"))
      .show()

  }

  /**
    * 按试卷分组获取每份试卷的分数前三用户详情
    * @param session
    * @param dt
    */
  def saveTop3UserDetailAPI(session:SparkSession,dt:String)= {
    session.
      sql("select * from dws.dws_user_paper_detail ")
      .where(s"dt=${dt}")
      .select("userid", "paperviewid", "paperviewname", "chaptername",
        "pointname", "sitecoursename", "coursename", "majorname", "shortname",
        "papername", "score", "dt", "dn")
      .withColumn("rk", dense_rank().over(Window.partitionBy("paperviewid")
        .orderBy(desc("score"))))
      .where("rk<4")
      .select("userid", "paperviewid", "paperviewname", "chaptername", "pointname", "sitecoursename"
        , "coursename", "majorname", "shortname", "papername", "score", "rk", "dt", "dn")
      .show()
  }

  /**
    * 按试卷分组获取每份试卷的分数后三用户详情
    * @param session
    * @param dt
    */
  def saveLow3UserDetailAPI(session:SparkSession,dt:String)= {
    session.sql("select * from dws.dws_user_paper_detail")
        .where(s"dt=${dt}")
        .select("userid","paperviewid","paperviewname","chaptername","pointname",
        "sitecoursename","coursename","majorname","shortname","papername","score","dt","dn")
      .withColumn("rk",dense_rank().over(Window.partitionBy("paperviewid").orderBy("score")))
      .where("rk<4")
      .select("userid", "paperviewid", "paperviewname", "chaptername", "pointname", "sitecoursename"
        , "coursename", "majorname", "shortname", "papername", "score", "rk", "dt", "dn")
      .show()

  }
  /**
    * 统计各试卷各分段的用户id，分段有0-20,20-40,40-60，60-80,80-100
    * @param session
    * @param dt
    */
  def savePaperScoreSegmentUserAPI(session:SparkSession,dt:String) ={
      session.sql("select paperviewid,paperviewname,userid,score,dt,dn from dws.dws_user_paper_detail")
      .where(s"dt=${dt}")
      .withColumn("score_segment",
        when(col("score").between(0,20),"0-20")
        .when(col("score")>20 && col("score")<=40,"20-40")
        .when(col("score")>40 && col("score")<=60,"40-60")
        .when(col("score")>60 && col("score")<=80,"60-80")
        .when(col("score")>80 && col("score")<=100,"80-100"))
      .drop("score")
      .groupBy("paperviewid","paperviewname","score_segment","dt","dn")
      .agg(concat_ws(",",collect_list(col("userid").cast("string"))).as("userids"))
      .select("paperviewid", "paperviewname", "score_segment", "userids", "dt", "dn")
      .orderBy("paperviewid","score_segment")
      .show()


//      .select("paperviewid","paperviewname","score_segment","userids","dt","dn")
//      .coalesce(1).write.mode(SaveMode.Overwrite).insertInto("ads.ads_paper_scoresegment_user")
  }

  /**
    *
    * 统计各试卷未及格人数 及格人数 及格率
    * @param session
    * @param dt
    */
  def savePaperPassDetailAPI(session: SparkSession, dt: String) = {
    val unPassdetail = session.sql("select paperviewid,paperviewname,dt,dn from dws.dws_user_paper_detail")
      .where(s"dt=${dt}").where("score between 0 and 60")
      .groupBy("paperviewid", "paperviewname", "dt", "dn")
      .agg(count("paperviewid").as("unpasscount"))

    val passDetail = session.sql("select paperviewid,dn from dws.dws_user_paper_detail")
      .where(s"dt=${dt}").where("score>60")
      .groupBy("paperviewid",   "dn")
      .agg(count("paperviewid").as("passcount"))

      unPassdetail.join(passDetail,Seq("dn","paperviewid"))
      .withColumn("rate",(col("passcount")./(col("passcount")+col("unpasscount"))).cast("decimal(4,2)"))
      .select("paperviewid","paperviewname","unpasscount","passcount","rate","dt","dn")
      .show()



  }

  /**
    * 统计各题 正确人数 错误人数 错题率
    * @param session
    * @param dt
    */
  def saveQuestionDetailAPI(session: SparkSession, dt: String)={
    val errcount = session.sql("select questionid,dt,dn from dws.dws_user_paper_detail")
      .where(s"dt=${dt}").where("user_question_answer='0'")
      .groupBy("questionid", "dn", "dt")
      .agg(count("questionid").as("errcount"))

    val rightcount = session.sql("select questionid,dn from dws.dws_user_paper_detail")
      .where(s"dt=${dt}").where("user_question_answer='1'")
      .groupBy("questionid", "dn")
      .agg(count("questionid").as("rightcount"))

    errcount.join(rightcount,Seq("questionid","dn"))
      .withColumn("rate",(col("rightcount")/(col("rightcount")+col("errcount"))).cast("decimal(4,2)"))
      .show()


  }
}
