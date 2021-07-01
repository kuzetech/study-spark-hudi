package com.kuze.bigdata.study.shizhan.utils

object StringUtil {

  def isEmpty(str: String): Boolean = {
    if(str == null || str.length == 0){
      return true
    }
    false
  }

  def noEmpty(str: String): Boolean = {
    if(str == null || str.length == 0){
      return false
    }
    true
  }

}
