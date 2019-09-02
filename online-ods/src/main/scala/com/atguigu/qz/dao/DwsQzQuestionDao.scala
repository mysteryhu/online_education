package com.atguigu.qz.dao

import org.apache.spark.sql.SparkSession

object DwsQzQuestionDao {

  def getDwdQzQuestion(session:SparkSession,dt:String)={
    session.sql(s"select questionid,parentid,questypeid,quesviewtype," +
      s"content,answer,analysis,limitminute,score,splitscore,status," +
      s"optnum,lecture,creator,createtime,modifystatus,attanswer,questag," +
      s"vanalysisaddr,difficulty,quesskill,vdeoaddr,dt," +
      s"dn from dwd.dwd_qz_question where dt ='${dt}'")
  }

  def getDwdQzQuestionType(session:SparkSession,dt:String)={
    session.sql(s"select quesviewtype,viewtypename,description," +
      s"papertypename,splitscoretype,dn from " +
      s"dwd.dwd_qz_question_type where dt='${dt}'")
  }

}
