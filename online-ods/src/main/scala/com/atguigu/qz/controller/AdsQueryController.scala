package com.atguigu.qz.controller

import com.atguigu.qz.dao.AdsQzDao
import com.atguigu.util.{HiveUtil, SCUtil}

object AdsQueryController {

  def main(args: Array[String]): Unit = {
    val session = SCUtil.getSC()
    HiveUtil.openDynamicPartition(session)
//    HiveUtil.openCompression(session)
//    HiveUtil.useSnappyCompression(session)

//    AdsQzDao.getAdsPaperAvgTimeAndScore(session,"20190722")
//    AdsQzDao.getTopScore(session,"20190722")
//    AdsQzDao.getTop3UserDetail(session,"20190722").show()
//    AdsQzDao.getLow3UserDetail(session,"20190722")
//    AdsQzDao.getPaperScoreSegmentUser(session,"20190722")
//    AdsQzDao.getPaperPassDetail(session,"20190722")
//    AdsQzDao.getQuestionDetail(session,"20190722")


  }
}
