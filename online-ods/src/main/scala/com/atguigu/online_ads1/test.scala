package com.atguigu.online_ads1

import com.atguigu.online_dws1.DWS_Member
import com.atguigu.util.SCUtil

object test {
  def main(args: Array[String]): Unit = {
    val dt = "20190722"
    val session = SCUtil.getSC()
    DWS_Member.insertMember(session,dt)
//    val query4 = DemandTable.demand10(dt)
//    session.sql(query4).show()
  }

}
