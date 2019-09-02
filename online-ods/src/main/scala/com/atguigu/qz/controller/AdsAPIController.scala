package com.atguigu.qz.controller

import com.atguigu.qz.service.AdsQzAPIService
import com.atguigu.util.SCUtil

object AdsAPIController {

  def main(args: Array[String]): Unit = {
    val session = SCUtil.getSC()
    val dt ="20190722"
//    AdsQzAPIService.saveAdsPaperPassDetailAPI(session,dt)
//    AdsQzAPIService.saveTopScoreAPI(session,dt)
//    AdsQzAPIService.saveTop3UserDetailAPI(session,dt)
//    AdsQzAPIService.saveLow3UserDetailAPI(session,dt)
//    AdsQzAPIService.savePaperScoreSegmentUserAPI(session,dt)
//    AdsQzAPIService.savePaperPassDetailAPI(session,dt)
    AdsQzAPIService.saveQuestionDetailAPI(session,dt)

  }
}
