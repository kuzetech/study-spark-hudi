package com.kuze.bigdata.study.shizhan.domain

import scala.beans.BeanProperty

class IpAddressRelation extends Serializable {

  @BeanProperty var startNum: Long = 0L
  @BeanProperty var endNum: Long = 0L
  @BeanProperty var country: String = "未知"
  @BeanProperty var province: String = "未知"
  @BeanProperty var city: String = "未知"
  @BeanProperty var continent: String = "未知"

}
