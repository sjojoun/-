package com.ds.offline

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

/**
 * 离线数据处理 - 任务一：数据抽取
 * 从MySQL ds_db01库抽取增量数据到Hive ods库
 */
object Task1_Extract {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Task1_Extract")
      .enableHiveSupport()
      .getOrCreate()

    // 假设：任务要求分区为当前日期的前一天，若以脚本运行时可用入参传递，此处使用Spark内置日期函数计算
    // format: yyyyMMdd
    val etlDateStr = spark.sql("select date_format(date_sub(current_date(), 1), 'yyyyMMdd')").collect()(0).getString(0)

    val mysqlUrl = "jdbc:mysql://master:3306/ds_db01?useSSL=false&characterEncoding=utf8"
    val mysqlProps = new java.util.Properties()
    mysqlProps.put("user", "root")
    mysqlProps.put("password", "123456")

    // 建库ods
    spark.sql("CREATE DATABASE IF NOT EXISTS ods")

    // 需要抽取的表名列表
    val tables = Array("customer_inf", "product_info", "order_master", "order_detail")

    tables.foreach { tableName =>
      println(s"Start extracting table: $tableName")
      // 1. 读取MySQL数据
      val df = spark.read.jdbc(mysqlUrl, tableName, mysqlProps)

      // 添加分区字段 etl_date
      val dfWithPartition = df.withColumn("etl_date", lit(etlDateStr))

      // 2. 对于增量抽取，根据ods表中modified_time作为增量字段
      // 为了处理第一次全量或者后续的增量，我们可以检查Hive中是否已有该表
      val tableExists = spark.catalog.tableExists(s"ods.$tableName")

      val finalDf = if (tableExists) {
        // 如果表存在，获取ods表中最大的 modified_time
        val maxTimeRow = spark.sql(s"SELECT max(modified_time) FROM ods.$tableName").collect()
        if (maxTimeRow.length > 0 && maxTimeRow(0).get(0) != null) {
          val maxModifiedTime = maxTimeRow(0).getTimestamp(0)
          // 只保留新增或修改过的数据
          dfWithPartition.filter(col("modified_time") > lit(maxModifiedTime))
        } else {
          dfWithPartition
        }
      } else {
        dfWithPartition
      }

      // 3. 写入Hive ods库
      // 因为题目要求添加静态分区，且值固定为昨天的日期
      // 开启hive动态分区，以防万一
      spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")

      // 保存至Hive表 (使用Hive支持)
      finalDf.write
        .mode(SaveMode.Append)
        .partitionBy("etl_date")
        .format("hive")
        .saveAsTable(s"ods.$tableName")

      // 4. 使用hive cli执行show partitions命令 (在实际环境或bash脚本中执行，代码中打印提示)
      println(s"Data extracted to ods.$tableName. Execute 'show partitions ods.$tableName' in hive cli.")
    }

    spark.stop()
  }
}
