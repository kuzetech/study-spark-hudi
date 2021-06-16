package com.kuze.bigdata.study;

import org.apache.hudi.QuickstartUtils;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

public class HudiReadFromOss {
    public static void main(String[] args) throws IOException {


        SparkSession spark = SparkSession.builder().appName("Hoodie Datasource test")
                .master("local[2]")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.io.compression.codec", "snappy")
                .config("spark.sql.hive.convertMetastoreParquet", "false")
                .getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        String tableName = "hudi_trips_cow";
        String basePath = "/tmp/hudi_trips_cow";
        QuickstartUtils.DataGenerator dataGen = new QuickstartUtils.DataGenerator();

        /*List<String> inserts = convertToStringList(dataGen.generateInserts(10));
        Dataset<Row> df = spark.read().json(jsc.parallelize(inserts, 2));
        df.write().format("org.apache.hudi").
                options(getQuickstartWriteConfigs()).
                option(TABLE_NAME, tableName).
                mode(Overwrite).
                save(basePath);*/

        Dataset<Row> roViewDF = spark.read().format("org.apache.hudi").load(basePath + "/*/*/*");
        roViewDF.registerTempTable("hudi_ro_table");
        spark.sql("select * from hudi_ro_table").show(false);

        spark.stop();

    }
}
