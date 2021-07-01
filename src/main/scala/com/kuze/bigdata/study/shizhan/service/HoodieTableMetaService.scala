package com.kuze.bigdata.study.shizhan.service

import com.kuze.bigdata.study.shizhan.constants.ApplicationConstants
import com.kuze.bigdata.study.shizhan.domain.TableMeta
import com.kuze.bigdata.study.shizhan.utils.StringUtil
import org.apache.avro.Schema
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.HoodieCommitMetadata
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.HoodieTimeline
import org.apache.hudi.exception.TableNotFoundException
import org.apache.hudi.{DataSourceUtils, HoodieSparkUtils}
import org.apache.spark.sql.SparkSession

class HoodieTableMetaService (val session: SparkSession, val tableMeta: TableMeta) {

  private def createHoodieTableMetaClient(): HoodieTableMetaClient = {
    val allPaths = List(tableMeta.partitionPath)
    val fs = FSUtils.getFs(allPaths.head, session.sparkContext.hadoopConfiguration)
    val globPaths = HoodieSparkUtils.checkAndGlobPathIfNecessary(allPaths, fs)
    val tablePath = DataSourceUtils.getTablePath(fs, globPaths.toArray)
    val metaClient = new HoodieTableMetaClient(fs.getConf, tablePath)
    metaClient
  }

  /**
    * 从增量表获取最后一次commit的时间戳
    *
    * @return
    */
  def getLastCommitTiming(): String = {
    var metaClient: HoodieTableMetaClient = null
    try {
      metaClient = createHoodieTableMetaClient()
    } catch {
      case e: TableNotFoundException => throw new RuntimeException("表不存在")
    }

    val timeline: HoodieTimeline = metaClient.getCommitsTimeline
    if (!timeline.lastInstant().isPresent) {
      return HoodieTableMetaService.BEGINNING_CONSUMER_TIMING
    }
    val time = timeline.lastInstant().get().getTimestamp
    println(s"增量表最后一次提交时间为${time}")
    time
  }

  /**
    * 从存量表元数据中，获取增量表消费进度
    *
    * @return
    */
  def getLastConsumerTimingMap(): Map[String, String] = {
    var metaClient: HoodieTableMetaClient = null
    try {
      metaClient = createHoodieTableMetaClient()
    } catch {
      case e: TableNotFoundException => return Map.empty
    }

    val timeline: HoodieTimeline = metaClient.getCommitsTimeline
    if (!timeline.lastInstant().isPresent) {
      return Map.empty
    }
    val instant = timeline.lastInstant().get()
    val commitMetadata = HoodieCommitMetadata.fromBytes(timeline.getInstantDetails(instant).get, classOf[HoodieCommitMetadata])
    val consumerStr = commitMetadata.getMetadata(ApplicationConstants.METADATA_LAST_CONSUMER_TIME_KEY)

    decodeConsumerString(consumerStr)
  }

  /**
    * 获取存量表数据avro schema
    *
    * @return
    */
  def getTableAvroSchema(): Schema = {
    var metaClient: HoodieTableMetaClient = null
    try {
      metaClient = createHoodieTableMetaClient()
    } catch {
      case e: TableNotFoundException => throw new RuntimeException("表不存在")
    }

    val timeline: HoodieTimeline = metaClient.getCommitsTimeline
    if (!timeline.lastInstant().isPresent) {
      throw new RuntimeException("不存在任何时间线")
    }

    val instant = timeline.lastInstant().get()
    val commitMetadata = HoodieCommitMetadata.fromBytes(timeline.getInstantDetails(instant).get, classOf[HoodieCommitMetadata])
    val avroSchemaStr = commitMetadata.getMetadata("schema")

    new Schema.Parser().parse(avroSchemaStr)
  }

  def getLastConsumerTiming(tableName: String): String = {
    val map = getLastConsumerTimingMap()
    if(map.isEmpty){
      println(s"增量表的最后一次消费时间节点为${HoodieTableMetaService.BEGINNING_CONSUMER_TIMING}")
      return HoodieTableMetaService.BEGINNING_CONSUMER_TIMING
    }
    val time = map.getOrElse(tableName,HoodieTableMetaService.BEGINNING_CONSUMER_TIMING)
    println(s"增量表的最后一次消费时间节点为${time}")
    time
  }

  def decodeConsumerString(consumerStr: String) = {
    if(StringUtil.isEmpty(consumerStr)){
      Map.empty[String,String]
    }else{
      val consumerGroup = consumerStr.split(",")
      val map: Map[String, String] = consumerGroup.map(table => {
        val tableNameAndTime = table.split("\\+")
        (tableNameAndTime(0), tableNameAndTime(1))
      }).toMap
      map
    }
  }

}

object HoodieTableMetaService {

  final val BEGINNING_CONSUMER_TIMING = "000"

  def encodeConsumerString(lastVersion: Map[String, String], submitVersion: Map[String, String]) = {
    val newMetaMap = scala.collection.mutable.Map[String,String]()

    submitVersion.foreach(item=>{
      newMetaMap.put(item._1,item._2)
    })

    lastVersion.foreach(item=>{
      if(submitVersion.get(item._1).isEmpty){
        newMetaMap.put(item._1,item._2)
      }
    })

    newMetaMap.toMap.toArray
      .map(table => table._1+"+"+table._2)
      .fold("")((a, b)=>a+","+b).substring(1)
  }

  def getLastConsumerTimingFromMap(metaMap: Map[String,String], tableName: String): String = {
    val timing = metaMap.getOrElse(tableName,HoodieTableMetaService.BEGINNING_CONSUMER_TIMING)
    println(s"增量表的最后一次消费时间节点为${timing}")
    timing
  }

}
