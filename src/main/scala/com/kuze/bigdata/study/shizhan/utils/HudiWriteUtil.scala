package com.kuze.bigdata.study.shizhan.utils

import com.xmfunny.hudi.AvroSchemaFieldComparator
import com.kuze.bigdata.study.shizhan.constants.ApplicationConstants
import com.kuze.bigdata.study.shizhan.domain.TableMeta
import com.kuze.bigdata.study.shizhan.service.HoodieTableMetaService
import org.apache.hudi.{AvroConversionUtils, DataSourceWriteOptions}
import org.apache.hudi.DataSourceWriteOptions.{PARTITIONPATH_FIELD_OPT_KEY, PRECOMBINE_FIELD_OPT_KEY, RECORDKEY_FIELD_OPT_KEY}
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.spark.sql.{DataFrame, SaveMode}

object HudiWriteUtil {


  def writeDataWithConsumerMeta(df: DataFrame, tableMeta: TableMeta, consumerMeta: String) = {
    write(df,tableMeta,getQuickstartWriteConfigs,SaveMode.Append,consumerMeta)
  }

  def overwriteDataWithConsumerMeta(df: DataFrame, tableMeta: TableMeta, consumerMeta: String) = {
    write(df,tableMeta,getQuickstartWriteConfigs,SaveMode.Overwrite,consumerMeta)
  }

  def writeData(df: DataFrame, tableMeta: TableMeta) = {
    write(df,tableMeta,getQuickstartWriteConfigs,SaveMode.Append,null)
  }

  def overwriteData(df: DataFrame, tableMeta: TableMeta) = {
    write(df,tableMeta,getQuickstartWriteConfigs,SaveMode.Overwrite,null)
  }

  def overwriteDataWithConfig(df: DataFrame, tableMeta: TableMeta, config: Map[String,String]) = {
    write(df,tableMeta,config,SaveMode.Overwrite,null)
  }

  def writeDataWithConfig(df: DataFrame, tableMeta: TableMeta, config: Map[String,String]) = {
    write(df,tableMeta,config,SaveMode.Append,null)
  }

  private def write(df: DataFrame, tableMeta: TableMeta, config: Map[String,String], saveMode: SaveMode, consumerMeta: String) = {

    //checkDataSchema(df, tableMeta)

    val writer = df.write.format("hudi")
      .options(config)
      .option(PRECOMBINE_FIELD_OPT_KEY, tableMeta.precombineFieldName)
      .option(RECORDKEY_FIELD_OPT_KEY, tableMeta.keyFieldName)
      .option(HoodieWriteConfig.TABLE_NAME, tableMeta.tableName)
      .mode(saveMode)

    if(StringUtil.noEmpty(consumerMeta)){
      writer.option(ApplicationConstants.METADATA_LAST_CONSUMER_TIME_KEY, consumerMeta)
    }

    if(StringUtil.isEmpty(tableMeta.partitionFieldName)){
      writer.option(DataSourceWriteOptions.KEYGENERATOR_CLASS_OPT_KEY, "org.apache.hudi.keygen.NonpartitionedKeyGenerator")
    }else{
      writer.option(PARTITIONPATH_FIELD_OPT_KEY, tableMeta.partitionFieldName)
    }

    writer.save(tableMeta.bathPath)
  }

  def checkDataSchema(df: DataFrame, tableMeta: TableMeta) = {

    val tableSchema = new HoodieTableMetaService(df.sparkSession, tableMeta).getTableAvroSchema()

    val structNameAndNameSpace = AvroConversionUtils.getAvroRecordNameAndNamespace(tableMeta.tableName)

    val dataSchema = AvroConversionUtils.convertStructTypeToAvroSchema(df.schema,structNameAndNameSpace._1,structNameAndNameSpace._2)

    tableSchema.getFields.sort(new AvroSchemaFieldComparator)
    dataSchema.getFields.sort(new AvroSchemaFieldComparator)

    if(!tableSchema.toString().equals(dataSchema.toString())){
      println(s"表结构为${tableSchema.toString()}")
      println(s"源结构为${dataSchema.toString()}")
      throw new RuntimeException("输入数据和存量表的数据结构不一致")
    }

  }

  def getQuickstartWriteConfigs: Map[String, String] = {
    val demoConfigs = Map(
      "hoodie.insert.shuffle.parallelism" -> "2",
      "hoodie.upsert.shuffle.parallelism"-> "2"
    )
    demoConfigs
  }

}
