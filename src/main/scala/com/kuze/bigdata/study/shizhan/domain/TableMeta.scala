package com.kuze.bigdata.study.shizhan.domain

import java.util.Properties

import com.kuze.bigdata.study.shizhan.utils.StringUtil
import org.apache.hadoop.fs.{FSDataOutputStream, Path}
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.spark.sql.SparkSession

class TableMeta (
                  val tableName: String,
                  val bathPath: String,
                  val partitionPath: String,
                  val precombineFieldName: String,
                  val keyFieldName: String,
                  val partitionFieldName: String) extends Serializable {

}


object TableMeta {

  val NO_PARTITION_FIELD = ""

  val USER_DEFINED_TABLE_META = HoodieTableMetaClient.AUXILIARYFOLDER_NAME + "/table/meta.properties"

  def apply(spark: SparkSession, bathPath: String):TableMeta = {
    val propertyPath = new Path(bathPath, TableMeta.USER_DEFINED_TABLE_META)
    val fs = FSUtils.getFs(bathPath, spark.sparkContext.hadoopConfiguration)
    val stream = fs.open(propertyPath)
    val newProps = new Properties
    newProps.load(stream)
    val tableName = newProps.getProperty("table.name")
    val tablePartition = newProps.getProperty("table.partition.path")
    val precombineFieldName = newProps.getProperty("table.field.name.precombine")
    val keyFieldName = newProps.getProperty("table.field.name.key")
    var partitionFieldName = newProps.getProperty("table.field.name.partition")
    if(StringUtil.isEmpty(partitionFieldName)){
      partitionFieldName = NO_PARTITION_FIELD
    }
    new TableMeta(tableName,bathPath,tablePartition,precombineFieldName,keyFieldName,partitionFieldName)
  }

}
