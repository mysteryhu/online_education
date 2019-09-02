package com.atguigu.qz.controller

import com.atguigu.qz.service.DwsQzService
import com.atguigu.util.{HiveUtil, SCUtil}

object DwsDataController {

  def main(args: Array[String]): Unit = {
    val session = SCUtil.getSC()
    HiveUtil.openDynamicPartition(session)
    HiveUtil.openCompression(session)
    HiveUtil.useSnappyCompression(session)
    DwsQzService.saveDwsQzChapter(session,"20190722")
//    DwsQzService.saveDwsQzMajor(session,"20190722")
//    DwsQzService.saveDwsQzCourse(session,"20190722")
//    DwsQzService.saveDwsQzPaper(session,"20190722")
//    DwsQzService.saveDwsQzQuestion(session,"20190722")
    DwsQzService.saveDwsUserPaperDetail(session,"20190722")

  }

}
