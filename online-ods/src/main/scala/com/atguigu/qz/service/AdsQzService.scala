package com.atguigu.qz.service

import com.atguigu.qz.dao.AdsQzDao
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


//TODO
object AdsQzService {

  def saveAdsPaperPassDetail(session: SparkSession, dt: String)={
    val adsPaperAvgTimeAndScore: DataFrame = AdsQzDao.getAdsPaperAvgTimeAndScore(session, dt)
    adsPaperAvgTimeAndScore.select("paperviewid","paperviewname","avgscore","avgspendtime","dt","dn")
      .coalesce(1).write.mode(SaveMode.Overwrite).insertInto("ads.ads_paper_avgtimeandscore")
  }

  def saveTopScore(session:SparkSession,dt:String)={
    AdsQzDao.getTopScore(session,dt)
      .select("paperviewid","paperviewname","maxscore","minscore","dt","dn")
      .coalesce(1).write.mode(SaveMode.Overwrite).insertInto("ads.ads_paper_maxdetail")
  }

  def saveTop3UserDetail(session:SparkSession,dt:String)={
    AdsQzDao.getTop3UserDetail(session,dt)
      .select("userid","paperviewid","paperviewname","chaptername","pointname","sitecoursename","coursename","majorname","shortname","papername","score","rk","dt","dn")
      .coalesce(1).write.mode(SaveMode.Overwrite).insertInto("ads.ads_top3_userdetail")
  }

  def saveLow3UserDetail(session:SparkSession,dt:String)= {
   AdsQzDao.getLow3UserDetail(session,dt)
      .select("userid", "paperviewid", "paperviewname", "chaptername", "pointname", "sitecoursename", "coursename", "majorname", "shortname", "papername", "score", "rk", "dt", "dn")
      .coalesce(1).write.mode(SaveMode.Overwrite).insertInto("ads.ads_low3_userdetail")
  }


  def savePaperPassDetail(session: SparkSession, dt: String) = {
    AdsQzDao.getPaperPassDetail(session,dt)
      .select("paperviewid","paperviewname","unpasscount","passcount","rate","dt","dn")
      .coalesce(1).write.mode(SaveMode.Overwrite).insertInto("ads.ads_user_paper_detail")
  }


  def saveQuestionDetail(session: SparkSession, dt: String)={
    AdsQzDao.getQuestionDetail(session,dt)
      .select("questionid","errcount","rightcount","rate","dt","dn")
      .coalesce(1).write.mode(SaveMode.Overwrite).insertInto("ads.ads_user_question_detail")

  }

}
