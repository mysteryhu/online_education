package com.atguigu.qz.dao

import org.apache.spark.sql.{SaveMode, SparkSession}

//TODO SparkSQL实现
object AdsQzDao {

  /**
    *基于宽表统计各试卷平均耗时、平均分
    * @param session
    * @param dt
    */
  def getAdsPaperAvgTimeAndScore(session:SparkSession,dt:String) ={
    session.sql(s"select paperviewid,paperviewname,cast(avg(score) as decimal(4,1)) avgscore,cast(avg(spendtime) as decimal(10,2)) avgspendtime,dt,dn from dws.dws_user_paper_detail where dt='${dt}' group by paperviewid,paperviewname,dn,dt order by avgscore desc,avgspendtime desc")
  }

  /**
    * 统计各试卷最高分、最低分
    * @param session
    * @param dt
    */
  def getTopScore(session:SparkSession,dt:String)={
    session.sql(s"select paperviewid,paperviewname,cast(max(score) as decimal(4,1)) as maxscore,\ncast(min(score) as decimal(4,1)) as minscore,dt,dn from  dws.dws_user_paper_detail\nwhere dt='${dt}'\ngroup by paperviewid,paperviewname,dn,dt")
  }

  /**
    * 按试卷分组获取每份试卷的分数前三用户详情
    * @param session
    * @param dt
    */
  def getTop3UserDetail(session:SparkSession,dt:String)={
    session.sql(s"select * from(select userid,paperviewid,paperviewname,chaptername,pointname,sitecoursename,coursename,majorname,shortname,papername,score,dense_rank() over(partition by paperviewid order by score desc) as rk,dt,dn from dws.dws_user_paper_detail where dt=${dt})r where rk<4")
  }

  /**
    * 按试卷分组获取每份试卷的分数后三用户详情
    * @param session
    * @param dt
    */
  def getLow3UserDetail(session:SparkSession,dt:String)= {
    session.sql(s"select * from(select userid,paperviewid,paperviewname,chaptername,pointname,sitecoursename,coursename,majorname,shortname,papername,score,dense_rank() over(partition by paperviewid order by score) as rk,dt,dn from dws.dws_user_paper_detail where dt='${dt}')r where rk<4")
  }
  /**
    * 统计各试卷各分段的用户id，分段有0-20,20-40,40-60，60-80,80-100
    * @param session
    * @param dt
    */
  def getPaperScoreSegmentUser(session:SparkSession,dt:String) ={
      session.sql(s"select paperviewid,paperviewname,score_segment,concat_ws(',',collect_list(cast(userid as string))) userids,dt,dn\nfrom \n(select paperviewid,paperviewname,\ncase when score >=0 and score <20 then '0-20'\n     when score >=20 and score <40 then '20-40'\n     when score >=40 and score <60 then '40-60'\n     when score >=60 and score <80 then '60-80'\n     when score >=80 and score <=100 then '80-100' end as score_segment,userid,\n     dt,dn from dws.dws_user_paper_detail\n     where dt='${dt}')r\n     group by paperviewid,paperviewname,score_segment,dt,dn order by paperviewid,score_segment")
      .select("paperviewid","paperviewname","score_segment","userids","dt","dn")
        .coalesce(1).write.mode(SaveMode.Overwrite).insertInto("ads.ads_paper_scoresegment_user")
  }

  /**
    *
    * 统计各试卷未及格人数 及格人数 及格率
    * @param session
    * @param dt
    */
  def getPaperPassDetail(session: SparkSession, dt: String) = {
    session.sql("select c.paperviewid as paperviewid,c.paperviewname as paperviewname,c.unpass as unpasscount,c.pass as passcount,cast((c.pass/(c.pass+c.unpass)) as decimal(4,2)) as rate,c.dt as dt,c.dn as dn from (" +
      "select a.paperviewid,a.paperviewname,a.unpass,b.pass,a.dt,a.dn from (" +
      s"select paperviewid,paperviewname,count(*) unpass,dt,dn from dws.dws_user_paper_detail where dt='${dt}' and score between 0 and 60 group by paperviewid,paperviewname,dt,dn)a join (" +
      s"select paperviewid,paperviewname,count(*) pass,dt,dn from dws.dws_user_paper_detail where dt='${dt}' \nand score >= 60 group by paperviewid,paperviewname,dt,dn\n)b on a.dn = b.dn and a.paperviewid=b.paperviewid)c")

  }

  /**
    * 统计各题 正确人数 错误人数 错题率 top3错误题数多的questionid
    * @param session
    * @param dt
    */
  def getQuestionDetail(session: SparkSession, dt: String)={
    session.sql("select questionid,errcount,rightcount,cast((errcount/(errcount+rightcount))as decimal(4,2)) as rate,dt,dn from (select a.questionid,a.errcount,b.rightcount,a.dt,a.dn from (" +
     s"select questionid,count(*) errcount,dt,dn from dws.dws_user_paper_detail\nwhere dt = '${dt}' and user_question_answer= '0' group by questionid,dt,dn)a join (" +
      s"select questionid,count(*) rightcount,dt,dn from dws.dws_user_paper_detail\nwhere dt = '${dt}' and user_question_answer= '1' group by questionid,dt,dn)b on a.dn =b.dn and a.questionid =b.questionid)c")

  }

}
