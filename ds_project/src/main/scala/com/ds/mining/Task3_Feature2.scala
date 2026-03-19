package com.ds.mining

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
 * 数据挖掘 - 任务三：特征工程（二）
 */
object Task3_Feature2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Task3_Feature2")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    val orderDetailDf = spark.table("dwd.fact_order_detail")

    // 1. 获取去重的用户购买商品记录，对customer_id和product_id进行mapping
    val userProducts = orderDetailDf.select("customer_id", "product_id").distinct()

    val custWin = Window.orderBy("customer_id")
    val prodWin = Window.orderBy("product_id")

    // Mapping: 按升序排列生成从0开始的索引键
    val customerMapping = userProducts.select("customer_id").distinct()
      .withColumn("cust_mapped", row_number().over(custWin) - 1)

    val productMapping = userProducts.select("product_id").distinct()
      .withColumn("prod_mapped", row_number().over(prodWin) - 1)

    val mappedDf = userProducts
      .join(customerMapping, Seq("customer_id"))
      .join(productMapping, Seq("product_id"))
      .select($"cust_mapped".as("customer_id"), $"prod_mapped".as("product_id"))
      .orderBy("customer_id", "product_id")

    println("------- customer_id_mapping与product_id_mapping数据前5条如下：-------")
    mappedDf.take(5).foreach(r => println(s"${r.getInt(0)}:${r.getInt(1)}"))

    // 2. 将product_id进行one-hot转换，转换为用户商品矩阵
    // 第一列为用户id，其余列名为商品id
    val oneHotDf = mappedDf
      .withColumn("value", lit(1.0))
      .groupBy("customer_id")
      .pivot("product_id")
      .agg(first("value"))
      .na.fill(0.0)

    val cols = oneHotDf.columns.filter(_ != "customer_id").sorted

    // 生成带前缀的列名
    val renamedDf = cols.foldLeft(oneHotDf) { (df, colName) =>
      df.withColumnRenamed(colName, s"product_id$colName")
    }.orderBy("customer_id")

    println("---------------第一行前5列结果展示为---------------")
    val firstRow = renamedDf.first()

    // 展示 customer_id 以及前 4 个 product_id
    val first5Cols = (0 to 4).map(i => {
      val c = renamedDf.columns(i)
      firstRow.getAs[Any](c).toString
    }).mkString(",")

    println(first5Cols)

    spark.stop()
  }
}
