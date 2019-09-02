package com.atguigu.online_bean

import java.sql.Timestamp

case class Dws_member(
                       uid: Int,
                       ad_id: Int,
                       fullname: String,
                       iconurl: String,
                       lastlogin: String,
                       mailaddr: String,
                       memberlevel: String,
                       password: String,
                       paymoney: String,
                       phone: String,
                       qq: String,
                       register: String,
                       regupdatetime: String,
                       unitname: String,
                       userip: String,
                       zipcode: String,
                       appkey: String,
                       appregurl: String,
                       bdp_uuid: String,
                       reg_createtime: String,
                       domain: String,
                       isranreg: String,
                       regsource: String,
                       regsourcename: String,
                       adname: String,
                       siteid: Int,
                       sitename: String,
                       siteurl: String,
                       site_delete: String,
                       site_createtime: String,
                       site_creator: String,
                       vip_id: Int,
                       vip_level: String,
                       vip_start_time: String,
                       vip_end_time: String,
                       vip_last_modify_time: String,
                       vip_max_free: String,
                       vip_min_free: String,
                       vip_next_level: String,
                       vip_operator: String,
                       dt: String,
                       dn: String
                     ) {}

case class dws_member_zipper(
                              uid: Int,
                              var paymoney: String,
                              vip_level: String,
                              var start_time: String,
                              var end_time: String,
                              dn: String
                            ) {}

case class MemberZipperList(list:List[dws_member_zipper]){}
