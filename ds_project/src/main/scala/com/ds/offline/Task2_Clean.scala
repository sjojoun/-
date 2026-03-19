package com.ds.offline

import org.apache.spark.sql.{SaveMode, SparkSession, DataFrame}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType

/**
 * 离线数据处理 - 任务二：数据清洗
 * 将ods库中相应表数据抽取到Hive的dwd库中
 */
object Task2_Clean {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Task2_Clean")
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("CREATE DATABASE IF NOT EXISTS dwd")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    val etlDateStr = spark.sql("select date_format(date_sub(current_date(), 1), 'yyyyMMdd')").collect()(0).getString(0)
    val currentTimeStr = spark.sql("select current_timestamp()").collect()(0).getTimestamp(0)

    import spark.implicits._

    // 格式化时间戳的工具函数
    def formatTimestamp(df: DataFrame, colNames: Seq[String]): DataFrame = {
      var resDf = df
      colNames.foreach { colName =>
        if (resDf.columns.contains(colName)) {
          // 若只有年月日，则补全时分秒，并转为 timestamp 类型
          resDf = resDf.withColumn(colName,
            when(length(col(colName)) === 10, to_timestamp(concat(col(colName), lit(" 00:00:00")), "yyyy-MM-dd HH:mm:ss"))
            .otherwise(to_timestamp(col(colName), "yyyy-MM-dd HH:mm:ss"))
          )
        }
      }
      resDf
    }

    // --- 1. dim_customer_inf ---
    // 抽取昨天ods数据，并与dim_customer_inf最新分区数据合并，按customer_id
    if (spark.catalog.tableExists("ods.customer_inf")) {
      val odsCustomer = spark.table("ods.customer_inf").filter($"etl_date" === etlDateStr)
      val formattedOdsCust = formatTimestamp(odsCustomer, Seq("register_time", "modified_time"))

      val (dimExists, existingDim) = if (spark.catalog.tableExists("dwd.dim_customer_inf")) {
        (true, spark.table("dwd.dim_customer_inf"))
      } else {
        (false, spark.emptyDataFrame)
      }

      val mergedCustomer = if (dimExists && existingDim.count() > 0) {
        val windowSpec = Window.partitionBy("customer_id").orderBy($"modified_time".desc_nulls_last)
        // 合并 ods 和 dwd 最新分区数据
        val unionDf = formattedOdsCust.unionByName(existingDim, allowMissingColumns = true)

        // 排序取最新
        val rankedDf = unionDf.withColumn("rn", row_number().over(windowSpec)).filter($"rn" === 1).drop("rn")

        // 区分是新增还是修改
        rankedDf.withColumn("dwd_insert_user", lit("user1"))
          .withColumn("dwd_modify_user", lit("user1"))
          .withColumn("dwd_insert_time",
            when($"dwd_insert_time".isNotNull, $"dwd_insert_time").otherwise(lit(currentTimeStr)))
          .withColumn("dwd_modify_time", lit(currentTimeStr))
      } else {
        formattedOdsCust
          .withColumn("dwd_insert_user", lit("user1"))
          .withColumn("dwd_modify_user", lit("user1"))
          .withColumn("dwd_insert_time", lit(currentTimeStr))
          .withColumn("dwd_modify_time", lit(currentTimeStr))
      }

      mergedCustomer.write.mode(SaveMode.Overwrite)
        .partitionBy("etl_date")
        .format("hive")
        .saveAsTable("dwd.dim_customer_inf")
    }

    // --- 2. dim_product_info ---
    if (spark.catalog.tableExists("ods.product_info")) {
      val odsProduct = spark.table("ods.product_info").filter($"etl_date" === etlDateStr)
      val formattedOdsProd = formatTimestamp(odsProduct, Seq("publish_time", "modified_time"))

      val (dimExists, existingDim) = if (spark.catalog.tableExists("dwd.dim_product_info")) {
        (true, spark.table("dwd.dim_product_info"))
      } else {
        (false, spark.emptyDataFrame)
      }

      val mergedProduct = if (dimExists && existingDim.count() > 0) {
        val windowSpec = Window.partitionBy("product_id").orderBy($"modified_time".desc_nulls_last)
        val unionDf = formattedOdsProd.unionByName(existingDim, allowMissingColumns = true)
        val rankedDf = unionDf.withColumn("rn", row_number().over(windowSpec)).filter($"rn" === 1).drop("rn")

        rankedDf.withColumn("dwd_insert_user", lit("user1"))
          .withColumn("dwd_modify_user", lit("user1"))
          .withColumn("dwd_insert_time",
            when($"dwd_insert_time".isNotNull, $"dwd_insert_time").otherwise(lit(currentTimeStr)))
          .withColumn("dwd_modify_time", lit(currentTimeStr))
      } else {
        formattedOdsProd
          .withColumn("dwd_insert_user", lit("user1"))
          .withColumn("dwd_modify_user", lit("user1"))
          .withColumn("dwd_insert_time", lit(currentTimeStr))
          .withColumn("dwd_modify_time", lit(currentTimeStr))
      }

      mergedProduct.write.mode(SaveMode.Overwrite)
        .partitionBy("etl_date")
        .format("hive")
        .saveAsTable("dwd.dim_product_info")
    }

    // --- 3. fact_order_master ---
    if (spark.catalog.tableExists("ods.order_master")) {
      val odsOrderMaster = spark.table("ods.order_master").filter($"etl_date" === etlDateStr)
      // 过滤 city 字段长度大于 8
      val filteredOdsOrderMaster = odsOrderMaster.filter(length($"city") <= 8)
      val formattedOdsOM = formatTimestamp(filteredOdsOrderMaster, Seq("create_time", "modified_time", "pay_time", "shipping_time", "receive_time"))

      val dwdOM = formattedOdsOM
        .withColumn("etl_date", date_format($"create_time", "yyyyMMdd")) // 重新取create_time为动态分区
        .withColumn("dwd_insert_user", lit("user1"))
        .withColumn("dwd_modify_user", lit("user1"))
        .withColumn("dwd_insert_time", lit(currentTimeStr))
        .withColumn("dwd_modify_time", lit(currentTimeStr))

      dwdOM.write.mode(SaveMode.Append)
        .partitionBy("etl_date")
        .format("hive")
        .saveAsTable("dwd.fact_order_master")
    }

    // --- 4. fact_order_detail ---
    if (spark.catalog.tableExists("ods.order_detail")) {
      val odsOrderDetail = spark.table("ods.order_detail").filter($"etl_date" === etlDateStr)
      val formattedOdsOD = formatTimestamp(odsOrderDetail, Seq("create_time", "modified_time"))

      val dwdOD = formattedOdsOD
        .withColumn("etl_date", date_format($"create_time", "yyyyMMdd")) // 重新取create_time为动态分区
        .withColumn("dwd_insert_user", lit("user1"))
        .withColumn("dwd_modify_user", lit("user1"))
        .withColumn("dwd_insert_time", lit(currentTimeStr))
        .withColumn("dwd_modify_time", lit(currentTimeStr))

      dwdOD.write.mode(SaveMode.Append)
        .partitionBy("etl_date")
        .format("hive")
        .saveAsTable("dwd.fact_order_detail")
    }

    spark.stop()
  }
}
