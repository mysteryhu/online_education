package com.atguigu.qz.dao

import org.apache.spark.sql.SparkSession

object DwsQzCourseDao {


  def getDwdQzSiteCourse(sparkSession: SparkSession, dt: String) ={
    sparkSession.sql(s"select sitecourseid,siteid,courseid,sitecoursename," +
      s"coursechapter,sequence,status,creator as sitecourse_creator," +
      s"createtime as sitecourse_createtime,helppaperstatus,servertype," +
      s"boardid,showstatus,dt,dn from dwd.dwd_qz_site_course " +
      s"where dt='${dt}'")
  }

  def getDwdQzCourse(sparkSession: SparkSession, dt: String) ={
    sparkSession.sql(s"select courseid,majorid,coursename,isadvc,chapterlistid,pointlistid,dn from dwd.dwd_qz_course where dt='${dt}'")
  }

  def getDwdQzCourseEdusubject(sparkSession: SparkSession, dt: String)= {
    sparkSession.sql(s"select courseid,courseeduid,edusubjectid,dn from dwd.dwd_qz_course_edusubject where dt='${dt}'")
  }
}
