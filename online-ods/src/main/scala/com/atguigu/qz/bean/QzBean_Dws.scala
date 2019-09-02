package com.atguigu.qz.bean

/**
  * dws.dws_qz_chapte : 4张表join  dwd.dwd_qz_chapter  inner join  dwd.qz_chapter_list
  * join条件：chapterlistid和dn ，inner join  dwd.dwd_qz_point
  * join条件：chapterid和dn,  inner join  dwd.dwd_qz_point_question
  * join条件：pointid和dn
  * 章节维度表
  * @param chapterid
  * @param chapterlistid
  * @param chaptername
  * @param sequence
  * @param showstatus
  * @param status
  * @param chapter_creator
  * @param chapter_createtime
  * @param chapter_courseid
  * @param chapternum
  * @param chapterallnum
  * @param outchapterid
  * @param chapterlistname
  * @param pointid
  * @param questionid
  * @param questype
  * @param pointname
  * @param pointyear
  * @param chapter
  * @param excisenum
  * @param pointlistid
  * @param pointdescribe
  * @param pointlevel
  * @param typelist
  * @param point_score
  * @param thought
  * @param remid
  * @param pointnamelist
  * @param typelistids
  * @param pointlist
  * @param dt
  * @param dn
  */
case class DwsQzChapter(
                         chapterid :Int,
                         chapterlistid :Int,
                         chaptername :String,
                         sequence :String,
                         showstatus :String,
                         status :String,
                         chapter_creator :String,
                         chapter_createtime :String,
                         chapter_courseid :Int,
                         chapternum :Int,
                         chapterallnum :Int,
                         outchapterid :Int,
                         chapterlistname :String,
                         pointid :Int,
                         questionid :Int,
                         questype :Int,
                         pointname :String,
                         pointyear :String,
                         chapter :String,
                         excisenum :Int,
                         pointlistid :Int,
                         pointdescribe :String,
                         pointlevel :String,
                         typelist :String,
                         var point_score :String,
                         thought :String,
                         remid :String,
                         pointnamelist :String,
                         typelistids :String,
                         pointlist :String,
                         dt :String,
                         dn :String
                       ){}

/**
  * 3张表join  dwd.dwd_qz_site_course inner join  dwd.qz_course
  * join条件：courseid和dn , inner join dwd.qz_course_edusubject
  * join条件:courseid和dn
  * 课程维度表
  * @param sitecourseid
  * @param siteid
  * @param courseid
  * @param sitecoursename
  * @param coursechapter
  * @param sequence
  * @param status
  * @param sitecourse_creator
  * @param sitecourse_createtime
  * @param helppaperstatus
  * @param servertype
  * @param boardid
  * @param showstatus
  * @param majorid
  * @param coursename
  * @param isadvc
  * @param chapterlistid
  * @param pointlistid
  * @param courseeduid
  * @param edusubjectid
  * @param dt
  * @param dn
  */
case class DwsQzCourse(
                        sitecourseid :Int,
                        siteid :Int,
                        courseid :Int,
                        sitecoursename :String,
                        coursechapter  :String,
                        sequence :String,
                        status :String,
                        sitecourse_creator :String,
                        sitecourse_createtime :String,
                        helppaperstatus :String,
                        servertype :String,
                        boardid :Int,
                        showstatus :String,
                        majorid :Int,
                        coursename :String,
                        isadvc :String,
                        chapterlistid :Int,
                        pointlistid :Int,
                        courseeduid :Int,
                        edusubjectid :Int,
                        dt :String,
                        dn :String
                      ){}

/**
  * 3张表join  dwd.dwd_qz_major  inner join  dwd.dwd_qz_website
  * join条件：siteid和dn , inner join dwd.dwd_qz_business
  * join条件：businessid和dn
  * 主修维度表
  * @param majorid
  * @param businessid
  * @param siteid
  * @param majorname
  * @param shortname
  * @param status
  * @param sequence
  * @param major_creator
  * @param major_createtime
  * @param businessname
  * @param sitename
  * @param domain
  * @param multicastserver
  * @param templateserver
  * @param multicastgateway
  * @param multicastport
  * @param dt
  * @param dn
  */
case class DwsQzMajor(
                       majorid :Int,
                       businessid :Int,
                       siteid :Int,
                       majorname :String,
                       shortname :String,
                       status :String,
                       sequence :String,
                       major_creator :String,
                       major_createtime :String,
                       businessname :String,
                       sitename :String,
                       domain :String,
                       multicastserver :String,
                       templateserver :String,
                       multicastgateway :String,
                       multicastport :String,
                       dt :String,
                       dn :String
                     ){}

/**
  * 4张表join:  qz_paperview left join qz_center
  * join 条件：paperviewid和dn,
  * left join qz_center
  * join 条件：centerid和dn, inner join qz_paper
  * join条件：paperid和dn
  * 试卷维度表
  * @param paperviewid
  * @param paperid
  * @param paperviewname
  * @param paperparam
  * @param openstatus
  * @param explainurl
  * @param iscontest
  * @param contesttime
  * @param conteststarttime
  * @param contestendtime
  * @param contesttimelimit
  * @param dayiid
  * @param status
  * @param paper_view_creator
  * @param paper_view_createtime
  * @param paperviewcatid
  * @param modifystatus
  * @param description
  * @param paperuse
  * @param paperdifficult
  * @param testreport
  * @param paperuseshow
  * @param centerid
  * @param sequence
  * @param centername
  * @param centeryear
  * @param centertype
  * @param provideuser
  * @param centerviewtype
  * @param stage
  * @param papercatid
  * @param courseid
  * @param paperyear
  * @param suitnum
  * @param papername
  * @param totalscore
  * @param chapterid
  * @param chapterlistid
  * @param dt
  * @param dn
  */
case class DwsQzPaper(
                       paperviewid :Int,
                       paperid :Int,
                       paperviewname :String,
                       paperparam :String,
                       openstatus :String,
                       explainurl :String,
                       iscontest :String,
                       contesttime :String,
                       conteststarttime :String,
                       contestendtime :String,
                       contesttimelimit :String,
                       dayiid :Int,
                       status :String,
                       paper_view_creator :String,
                       paper_view_createtime :String,
                       paperviewcatid :Int,
                       modifystatus :String,
                       description :String,
                       paperuse :String,
                       paperdifficult :String,
                       testreport :String,
                       paperuseshow :String,
                       centerid :Int,
                       sequence :String,
                       centername :String,
                       centeryear :String,
                       centertype :String,
                       provideuser :String,
                       centerviewtype :String,
                       stage :String,
                       papercatid :Int,
                       courseid :Int,
                       paperyear :String,
                       suitnum :String,
                       papername :String,
                       var totalscore :String,
                       chapterid :Int,
                       chapterlistid :Int,
                       dt :String,
                       dn:String
                     ){
}

/**
  * 2表join  qz_quesiton inner join qz_questiontype
  * join条件:questypeid 和dn
  * 题目维度表
  * @param questionid
  * @param parentid
  * @param questypeid
  * @param quesviewtype
  * @param content
  * @param answer
  * @param analysis
  * @param limitminute
  * @param score
  * @param splitscore
  * @param status
  * @param optnum
  * @param lecture
  * @param creator
  * @param createtime
  * @param modifystatus
  * @param attanswer
  * @param questag
  * @param vanalysisaddr
  * @param difficulty
  * @param quesskill
  * @param vdeoaddr
  * @param viewtypename
  * @param description
  * @param papertypename
  * @param splitscoretype
  * @param dt
  * @param dn
  */
case  class DwsQzQuestion(
                           questionid :Int,
                           parentid :Int,
                           questypeid :Int,
                           quesviewtype :Int,
                           content :String,
                           answer :String,
                           analysis :String,
                           limitminute :String,
                           var score :String,
                           var splitscore :String,
                           status :String,
                           optnum :Int,
                           lecture :String,
                           creator :String,
                           createtime :String,
                           modifystatus :String,
                           attanswer :String,
                           questag :String,
                           vanalysisaddr :String,
                           difficulty :String,
                           quesskill :String,
                           vdeoaddr :String,
                           viewtypename :String,
                           description :String,
                           papertypename :String,
                           splitscoretype :String,
                           dt :String,
                           dn :String
                         ){}

/**
  * 宽表合成
  * 需求3：基于dws.dws_qz_chapter、dws.dws_qz_course、dws.dws_qz_major、dws.dws_qz_paper、
  * dws.dws_qz_question、dwd.dwd_qz_member_paper_question 合成宽表dw.user_paper_detail,
  * sql和dataframe api操作
  * @param userid
  * @param courseid
  * @param questionid
  * @param useranswer
  * @param istrue
  * @param lasttime
  * @param opertype
  * @param paperid
  * @param spendtime
  * @param chapterid
  * @param chaptername
  * @param chapternum
  * @param chapterallnum
  * @param outchapterid
  * @param chapterlistname
  * @param pointid
  * @param questype
  * @param pointyear
  * @param chapter
  * @param pointname
  * @param excisenum
  * @param pointdescribe
  * @param pointlevel
  * @param typelist
  * @param point_score
  * @param thought
  * @param remid
  * @param pointnamelist
  * @param typelistids
  * @param pointlist
  * @param sitecourseid
  * @param siteid
  * @param sitecoursename
  * @param coursechapter
  * @param course_sequence
  * @param course_stauts
  * @param course_creator
  * @param course_createtime
  * @param servertype
  * @param helppaperstatus
  * @param boardid
  * @param showstatus
  * @param majorid
  * @param coursename
  * @param isadvc
  * @param chapterlistid
  * @param pointlistid
  * @param courseeduid
  * @param edusubjectid
  * @param businessid
  * @param majorname
  * @param shortname
  * @param major_status
  * @param major_sequence
  * @param major_creator
  * @param major_createtime
  * @param businessname
  * @param sitename
  * @param domain
  * @param multicastserver
  * @param templateserver
  * @param multicastgatway
  * @param multicastport
  * @param paperviewid
  * @param paperviewname
  * @param paperparam
  * @param openstatus
  * @param explainurl
  * @param iscontest
  * @param contesttime
  * @param conteststarttime
  * @param contestendtime
  * @param contesttimelimit
  * @param dayiid
  * @param paper_status
  * @param paper_view_creator
  * @param paper_view_createtime
  * @param paperviewcatid
  * @param modifystatus
  * @param description
  * @param paperuse
  * @param testreport
  * @param centerid
  * @param paper_sequence
  * @param centername
  * @param centeryear
  * @param centertype
  * @param provideuser
  * @param centerviewtype
  * @param paper_stage
  * @param papercatid
  * @param paperyear
  * @param suitnum
  * @param papername
  * @param totalscore
  * @param question_parentid
  * @param questypeid
  * @param quesviewtype
  * @param question_content
  * @param question_answer
  * @param question_analysis
  * @param question_limitminute
  * @param score
  * @param splitscore
  * @param lecture
  * @param question_creator
  * @param question_createtime
  * @param question_modifystatus
  * @param question_attanswer
  * @param question_questag
  * @param question_vanalysisaddr
  * @param question_difficulty
  * @param quesskill
  * @param vdeoaddr
  * @param question_description
  * @param question_splitscoretype
  * @param user_question_answer
  * @param dt
  * @param dn
  */
case class dws_user_paper_detail(
                                  userid :Int,
                                  courseid: Int,
                                  questionid: Int,
                                  useranswer: String,
                                  istrue :String,
                                  lasttime: String,
                                  opertype: String,
                                  paperid :Int,
                                  spendtime: Int,
                                  chapterid: Int,
                                  chaptername: String,
                                  chapternum: Int,
                                  chapterallnum: Int,
                                  outchapterid: Int,
                                  chapterlistname: String,
                                  pointid :Int,
                                  questype :Int,
                                  pointyear: String,
                                  chapter :String,
                                  pointname: String,
                                  excisenum: Int,
                                  pointdescribe: String,
                                  pointlevel: String,
                                  typelist: String,
                                  var point_score: String,
                                  thought: String,
                                  remid: String,
                                  pointnamelist: String,
                                  typelistids: String,
                                  pointlist: String,
                                  sitecourseid: Int,
                                  siteid :Int,
                                  sitecoursename: String,
                                  coursechapter :String,
                                  course_sequence :String,
                                  course_stauts :String,
                                  course_creator: String,
                                  course_createtime: String,
                                  servertype :String,
                                  helppaperstatus: String,
                                  boardid :Int,
                                  showstatus :String,
                                  majorid :Int,
                                  coursename :String,
                                  isadvc :String,
                                  chapterlistid :Int,
                                  pointlistid :Int,
                                  courseeduid: Int,
                                  edusubjectid: Int,
                                  businessid :Int,
                                  majorname: String,
                                  shortname: String,
                                  major_status: String,
                                  major_sequence: String,
                                  major_creator :String,
                                  major_createtime: String,
                                  businessname:String,
                                  sitename:String,
                                  domain: String,
                                  multicastserver: String,
                                  templateserver: String,
                                  multicastgatway: String,
                                  multicastport: String,
                                  paperviewid: Int,
                                  paperviewname: String,
                                  paperparam:String,
                                  openstatus: String,
                                  explainurl: String,
                                  iscontest: String,
                                  contesttime: String,
                                  conteststarttime: String,
                                  contestendtime: String,
                                  contesttimelimit: String,
                                  dayiid: Int,
                                  paper_status: String,
                                  paper_view_creator: String,
                                  paper_view_createtime: String,
                                  paperviewcatid:Int,
                                  modifystatus: String,
                                  description: String,
                                  paperuse: String,
                                  testreport: String,
                                  centerid: Int,
                                  paper_sequence: String,
                                  centername:String,
                                  centeryear: String,
                                  centertype: String,
                                  provideuser: String,
                                  centerviewtype: String,
                                  paper_stage: String,
                                  papercatid: Int,
                                  paperyear: String,
                                  suitnum: String,
                                  papername: String,
                                  var totalscore: String,
                                  question_parentid: Int,
                                  questypeid: Int,
                                  quesviewtype:Int,
                                  question_content: String,
                                  question_answer: String,
                                  question_analysis: String,
                                  question_limitminute: String,
                                  var score: String,
                                  var splitscore: String,
                                  lecture: String,
                                  question_creator: String,
                                  question_createtime: String,
                                  question_modifystatus: String,
                                  question_attanswer: String,
                                  question_questag:String,
                                  question_vanalysisaddr:String,
                                  question_difficulty: String,
                                  quesskill:String,
                                  vdeoaddr: String,
                                  question_description: String,
                                  question_splitscoretype: String,
                                  user_question_answer:Int,
                                  dt :String,
                                  dn :String
                                ){}