package com.atguigu.qz.service

import com.atguigu.qz.dao._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object DwsQzService {

  /**
    * 章节维度表
    * @param session
    * @param dt
    */
    def saveDwsQzChapter(session:SparkSession,dt:String): Unit ={
      val dwdQzChapter = DwsQzChapterDao.getDwdQzChapter(session,dt)
      val dwdQzChapterlist = DwsQzChapterDao.getDwdQzChapterList(session,dt)
      val dwdQzPoint = DwsQzChapterDao.getDwdQzPoint(session,dt)
      val dwdQzPointQuestion = DwsQzChapterDao.getDwdQzPointQuestion(session,dt)

      val result = dwdQzChapter.join(dwdQzChapterlist, Seq("chapterlistid", "dn"), joinType = "inner")
        .join(dwdQzPoint, Seq("chapterid", "dn"))
        .join(dwdQzPointQuestion, Seq("pointid", "dn"))
        .select("chapterid", "chapterlistid", "chaptername", "sequence", "showstatus", "status",
        "chapter_creator", "chapter_createtime", "chapter_courseid", "chapternum", "chapterallnum", "outchapterid", "chapterlistname",
        "pointid", "questionid", "questype", "pointname", "pointyear", "chapter", "excisenum", "pointlistid", "pointdescribe",
        "pointlevel", "typelist", "point_score", "thought", "remid", "pointnamelist", "typelistids", "pointlist", "dt", "dn")
      result.coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dws.dws_qz_chapter")
      result.show()


    }

  /**
    * 主修维度表
    * @param session
    * @param dt
    */
  def saveDwsQzMajor(session:SparkSession,dt:String): Unit ={
    val dwdQzMajor = DwsQzMajorDao.getDwdQzMajor(session,dt)
    val dwdQzBusiness = DwsQzMajorDao.getDwdQzBusiness(session,dt)
    val dwdQzWebsite = DwsQzMajorDao.getDwdQzWebsite(session,dt)

    val result: DataFrame = dwdQzMajor.join(dwdQzBusiness, Seq("businessid", "dn"))
      .join(dwdQzWebsite, Seq("siteid", "dn"))
    val frame = result.select("majorid", "businessid", "siteid", "majorname", "shortname",
      "status", "sequence", "major_creator", "major_createtime", "businessname", "sitename",
      "domain", "multicastserver", "templateserver", "multicastgateway", "multicastport", "dt", "dn")
    frame.show()
    frame.coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dws.dws_qz_major")
  }

  /**
    * 课程维度表
    * @param session
    * @param dt
    */
  def saveDwsQzCourse(session:SparkSession,dt:String): Unit ={
    val dwdQzSiteCourse = DwsQzCourseDao.getDwdQzSiteCourse(session,dt)
    val dwdQzCourse = DwsQzCourseDao.getDwdQzCourse(session,dt)
    val dwdQzCourseEdusubject = DwsQzCourseDao.getDwdQzCourseEdusubject(session,dt)

    val result = dwdQzSiteCourse.join(dwdQzCourse, Seq("courseid", "dn"))
      .join(dwdQzCourseEdusubject, Seq("courseid", "dn"))

    val  frame= result.select("sitecourseid", "siteid", "courseid", "sitecoursename",
      "coursechapter", "sequence", "status", "sitecourse_creator", "sitecourse_createtime",
      "helppaperstatus", "servertype", "boardid", "showstatus", "majorid", "coursename",
      "isadvc", "chapterlistid", "pointlistid", "courseeduid", "edusubjectid", "dt", "dn")
    frame.coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dws.dws_qz_course")
    frame.show()
  }

  /**
    * 试卷维度表
    * @param session
    * @param dt
    */
  def saveDwsQzPaper(session:SparkSession,dt:String): Unit = {
    val dwdQzPaperView = DwsQzPaperDao.getDwdQzPaperView(session, dt)
    val dwdQzCenterPaper = DwsQzPaperDao.getDwdQzCenterPaper(session, dt)
    val dwdQzCenter = DwsQzPaperDao.getDwdQzCenter(session, dt)
    val dwdQzPaper = DwsQzPaperDao.getDwdQzPaper(session, dt)

    val result = dwdQzPaperView.join(dwdQzCenterPaper, Seq("paperviewid", "dn"), "left")
      .join(dwdQzCenter, Seq("centerid", "dn"), "left")
      .join(dwdQzPaper, Seq("paperid", "dn"))

    val frame = result.select("paperviewid", "paperid", "paperviewname", "paperparam", "openstatus",
      "explainurl", "iscontest", "contesttime", "conteststarttime", "contestendtime",
      "contesttimelimit", "dayiid", "status", "paper_view_creator",
      "paper_view_createtime", "paperviewcatid", "modifystatus", "description",
      "paperuse", "paperdifficult", "testreport", "paperuseshow", "centerid",
      "sequence", "centername", "centeryear", "centertype", "provideuser",
      "centerviewtype", "stage", "papercatid", "courseid", "paperyear", "suitnum",
      "papername", "totalscore", "chapterid", "chapterlistid", "dt", "dn")
    frame.coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dws.dws_qz_paper")
    frame.show()

  }

  /**
    * 做题维度表
    * @param session
    * @param dt
    */
    def saveDwsQzQuestion(session:SparkSession,dt:String):Unit={
      val dwdQzQuestion = DwsQzQuestionDao.getDwdQzQuestion(session,dt)
      val dwdQzQuestionType = DwsQzQuestionDao.getDwdQzQuestionType(session,dt)

      val result: DataFrame = dwdQzQuestion.join(dwdQzQuestionType,Seq("quesviewtype","dn"))
      .select("questionid","parentid","questypeid","quesviewtype","content","answer","analysis","limitminute","score","splitscore","status","optnum","lecture","creator","createtime","modifystatus","attanswer","questag","vanalysisaddr","difficulty","quesskill","vdeoaddr","viewtypename","description","papertypename","splitscoretype","dt","dn")
      result.show()
      result.coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dws.dws_qz_question")
    }

  /**
    * 宽表
    * @param sparkSession
    * @param dt
    */
  def saveDwsUserPaperDetail(sparkSession: SparkSession, dt: String) = {
    //广播变量导包
    import org.apache.spark.sql.functions.broadcast
    val dwdQzMemberPaperQuestion = UserPaperDetailDao.getDwdQzMemberPaperQuestion(sparkSession, dt).drop("paperid")
      .withColumnRenamed("question_answer", "user_question_answer")
    val dwsQzChapter = UserPaperDetailDao.getDwsQzChapter(sparkSession, dt).drop("courseid")
    val dwsQzCourse = UserPaperDetailDao.getDwsQzCourse(sparkSession, dt).withColumnRenamed("sitecourse_creator", "course_creator")
      .withColumnRenamed("sitecourse_createtime", "course_createtime").drop("majorid")
      .drop("chapterlistid").drop("pointlistid")
    val dwsQzMajor = UserPaperDetailDao.getDwsQzMajor(sparkSession, dt)
    val dwsQzPaper = UserPaperDetailDao.getDwsQzPaper(sparkSession, dt).drop("courseid")
    val dwsQzQuestion = UserPaperDetailDao.getDwsQzQuestion(sparkSession, dt)
    dwdQzMemberPaperQuestion.join(dwsQzCourse, Seq("sitecourseid", "dn")).
      join(broadcast(dwsQzChapter), Seq("chapterid", "dn"))
      .join(broadcast(dwsQzMajor), Seq("majorid", "dn"))
      .join(broadcast(dwsQzPaper), Seq("paperviewid", "dn"))
      .join(broadcast(dwsQzQuestion), Seq("questionid", "dn"))
      .select("userid", "courseid", "questionid", "useranswer", "istrue", "lasttime", "opertype",
        "paperid", "spendtime", "chapterid", "chaptername", "chapternum",
        "chapterallnum", "outchapterid", "chapterlistname", "pointid", "questype", "pointyear", "chapter", "pointname"
        , "excisenum", "pointdescribe", "pointlevel", "typelist", "point_score", "thought", "remid", "pointnamelist",
        "typelistids", "pointlist", "sitecourseid", "siteid", "sitecoursename", "coursechapter", "course_sequence", "course_status"
        , "course_creator", "course_createtime", "servertype", "helppaperstatus", "boardid", "showstatus", "majorid", "coursename",
        "isadvc", "chapterlistid", "pointlistid", "courseeduid", "edusubjectid", "businessid", "majorname", "shortname",
        "major_status", "major_sequence", "major_creator", "major_createtime", "businessname", "sitename",
        "domain", "multicastserver", "templateserver", "multicastgateway", "multicastport", "paperviewid", "paperviewname", "paperparam",
        "openstatus", "explainurl", "iscontest", "contesttime", "conteststarttime", "contestendtime", "contesttimelimit",
        "dayiid", "paper_status", "paper_view_creator", "paper_view_createtime", "paperviewcatid", "modifystatus", "description", "paperuse",
        "testreport", "centerid", "paper_sequence", "centername", "centeryear", "centertype", "provideuser", "centerviewtype",
        "paper_stage", "papercatid", "paperyear", "suitnum", "papername", "totalscore", "question_parentid", "questypeid",
        "quesviewtype", "question_content", "question_answer", "question_analysis", "question_limitminute", "score",
        "splitscore", "lecture", "question_creator", "question_createtime", "question_modifystatus", "question_attanswer",
        "question_questag", "question_vanalysisaddr", "question_difficulty", "quesskill", "vdeoaddr", "question_description",
        "question_splitscoretype", "user_question_answer", "dt", "dn").repartition(6)
      .show()
    while(true){
      println("1")
    }
      //.write.mode(SaveMode.Overwrite).insertInto("dws.dws_user_paper_detail")
  }

}
