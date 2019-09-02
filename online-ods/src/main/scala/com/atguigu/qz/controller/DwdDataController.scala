package com.atguigu.qz.controller

import com.atguigu.qz.service.EtlDataService
import com.atguigu.util.{HiveUtil, SCUtil}

object DwdDataController {

  def main(args: Array[String]): Unit = {
    val session = SCUtil.getSC()
    HiveUtil.openDynamicPartition(session)  //开启动态分区
    HiveUtil.openCompression(session) //开启压缩
    HiveUtil.useSnappyCompression(session)  //开启snappy压缩
    EtlDataService.etlQzChapter(session)
    EtlDataService.etlQzChapterList(session)
    EtlDataService.etlQzPoint(session)
    EtlDataService.etlQzPointQuestion(session)
    EtlDataService.etlQzSiteCourse(session)
    EtlDataService.etlQzCourse(session)
    EtlDataService.etlQzCourseEdusubject(session)
    EtlDataService.etlQzWebsite(session)
    EtlDataService.etlQzMajor(session)
    EtlDataService.etlQzBusiness(session)
    EtlDataService.etlQzPaperView(session)
    EtlDataService.etlQzCenterPaper(session)
    EtlDataService.etlQzPaper(session)
    EtlDataService.etlQzCenter(session)
    EtlDataService.etlQzQuestion(session)
    EtlDataService.etlQzQuestionType(session)
    EtlDataService.etlQzMemberPaperQuestion(session)

  }
}
