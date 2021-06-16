package com.kuze.bigdata.study

import java.util

import com.kuze.bigdata.study.utils.SparkQuicklyStartUtil
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.HoodieCommitMetadata
import org.apache.hudi.{DataSourceUtils, HoodieSparkUtils}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.HoodieTimeline

object L5HoodieTableMetaClientDemo {

  def main(args: Array[String]): Unit = {

    val spark = SparkQuicklyStartUtil.getDefaultSparkSession()

    val allPaths = List(L1WriteDemo.PARTITION_PATH)
    val fs = FSUtils.getFs(allPaths.head, spark.sparkContext.hadoopConfiguration)
    val globPaths = HoodieSparkUtils.checkAndGlobPathIfNecessary(allPaths, fs)
    val tablePath = DataSourceUtils.getTablePath(fs, globPaths.toArray)
    val metaClient = new HoodieTableMetaClient(fs.getConf, tablePath)

    val timeline: HoodieTimeline = metaClient.getCommitsTimeline

    if (timeline.lastInstant().isPresent) {

      val instant = timeline.lastInstant().get()

      //[20210311123326__commit__COMPLETED]
      println(instant)

      val commitMetadata = HoodieCommitMetadata.fromBytes(timeline.getInstantDetails(instant).get, classOf[HoodieCommitMetadata])

      val extraMetadata1 = commitMetadata.getMetadata(L1WriteDemo.META_KEY)

      val extraMetadata: util.Map[String, String] = commitMetadata.getExtraMetadata

      val extraMetadata2 = extraMetadata.get(L1WriteDemo.META_KEY)

      // true
      println( extraMetadata1 == extraMetadata2)

      println(extraMetadata.get("schema"))

    }

  }

}
