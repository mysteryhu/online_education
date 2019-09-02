package com.atguigu.qz.dao

import org.apache.spark.sql.SparkSession


object DwsQzMajorDao {


  def getDwdQzMajor(session:SparkSession,dt:String) ={
    session.sql(s"select majorid,businessid,siteid,majorname,shortname," +
      s"status,sequence,creator as major_creator,createtime as major_createtime," +
      s"dt,dn from dwd.dwd_qz_major where dt='${dt}'")
  }

  def getDwdQzBusiness(session:SparkSession,dt:String) ={
    session.sql(s"select businessid,businessname,dn from dwd.dwd_qz_business where dt='${dt}'")
  }

  def getDwdQzWebsite(session:SparkSession,dt:String) ={
    session.sql(s"select siteid,sitename,domain,multicastserver," +
      s"templateserver,multicastgateway,multicastport,dn " +
      s"from dwd.dwd_qz_website where dt ='${dt}'")
  }
}
