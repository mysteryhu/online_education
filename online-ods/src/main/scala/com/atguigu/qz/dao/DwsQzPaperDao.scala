package com.atguigu.qz.dao

import org.apache.spark.sql.SparkSession

object DwsQzPaperDao {

  def getDwdQzPaperView(session:SparkSession,dt:String)={
    session.sql("select paperviewid,paperid,paperviewname,paperparam," +
      "openstatus,explainurl,iscontest,contesttime,conteststarttime," +
      "contestendtime,contesttimelimit,dayiid,status," +
      "creator as paper_view_creator,createtime as paper_view_createtime," +
      "paperviewcatid,modifystatus,description,paperuse,paperdifficult," +
      s"testreport,paperuseshow,dt,dn from dwd.dwd_qz_paper_view where dt='${dt}'")
  }

  def  getDwdQzCenterPaper(session:SparkSession,dt:String)={
    session.sql("select paperviewid,centerid,sequence,dn from " +
      s"dwd.dwd_qz_center_paper where dt='${dt}'")
  }

  def getDwdQzCenter(session:SparkSession,dt:String)={
    session.sql(s"select centerid,centername,centeryear,centertype," +
      s"provideuser,centerviewtype,stage,dn from dwd.dwd_qz_center where dt='${dt}'")
  }

  def getDwdQzPaper(session:SparkSession,dt:String)={
    session.sql(s"select paperid,papercatid,courseid,paperyear,chapter," +
      s"suitnum,papername,totalscore,chapterid,chapterlistid," +
      s"dn from dwd.dwd_qz_paper where dt='${dt}'")
  }

}
