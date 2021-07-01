package com.kuze.bigdata.study.shizhan.constants

object ApplicationConstants {

  val APPLICATION_NAME = "test"
  val APPLICATION_MASTER = "local[3]"

  //随数据写入一起存储到元数据的的消费时间点 key 前缀
  val LAST_CONSUMER_TIME_PREFIX_KEY = "_last_time_"
  val METADATA_LAST_CONSUMER_TIME_KEY = LAST_CONSUMER_TIME_PREFIX_KEY + APPLICATION_NAME

}
