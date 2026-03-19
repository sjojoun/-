package com.ds.mining

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{StandardScaler, OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.Row
import scala.collection.mutable.WrappedArray

/**
 * 数据挖掘 - 任务一：特征工程（一）
 */
object Task1_Feature1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Task1_Feature1")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    // 假设使用Hive dwd中的订单详情表（fact_order_detail）和商品维表（dim_product_info）、用户维表（dim_customer_inf）
    // 剔除用户id与商品id不存在现有维表中的记录
    val orderDetailDf = spark.table("dwd.fact_order_detail")
    val customerDim = spark.table("dwd.dim_customer_inf").select("customer_id")
    val productDim = spark.table("dwd.dim_product_info").select("product_id")

    // 内连接以剔除不存在的记录，并利用缓存优化
    val validOrders = orderDetailDf
      .join(customerDim, Seq("customer_id"), "inner")
      .join(productDim, Seq("product_id"), "inner")
      .cache()

    // 1. 计算与用户5811购买相同商品种类最多的前10位用户
    val targetUser = 5811
    val targetProductsDf = validOrders.filter($"customer_id" === targetUser).select("product_id").distinct()

    // 找出所有其他用户购买的商品，并与5811购买的商品进行交集统计
    val otherUsersProducts = validOrders.filter($"customer_id" =!= targetUser)
      .select("customer_id", "product_id").distinct()
      .join(targetProductsDf, Seq("product_id"), "inner") // 交集
      .groupBy("customer_id")
      .agg(count("product_id").as("same_product_count"))
      .orderBy($"same_product_count".desc)
      .limit(10)

    val top10Users = otherUsersProducts.select("customer_id").collect().map(_.get(0).toString).mkString(",")

    println("-------------------相同种类前10的id结果展示为：--------------------")
    println(top10Users)

    // 2. 特征工程：数值规范化、类别one-hot
    // 从 ds_db01.sku_info 或 dwd.dim_product_info 中获取 id, brand_id, price, weight, height, length, width, three_category_id
    val mysqlUrl = "jdbc:mysql://master:3306/ds_db01?useSSL=false&characterEncoding=utf8"
    val mysqlProps = new java.util.Properties()
    mysqlProps.put("user", "root")
    mysqlProps.put("password", "123456")

    val skuInfo = spark.read.jdbc(mysqlUrl, "sku_info", mysqlProps)
      .select("id", "brand_id", "price", "weight", "height", "length", "width", "three_category_id")
      .na.fill(0.0) // 填补空值

    // 预处理：将数值列组合成向量以进行 StandardScaler
    val numericCols = Array("price", "weight", "height", "length", "width", "three_category_id")

    // 因为我们需要转换类型
    val skuInfoDouble = numericCols.foldLeft(skuInfo) { (df, colName) =>
      df.withColumn(colName, col(colName).cast("double"))
    }

    val assembler = new VectorAssembler()
      .setInputCols(numericCols)
      .setOutputCol("numeric_features")

    val scaler = new StandardScaler()
      .setInputCol("numeric_features")
      .setOutputCol("scaled_features")
      .setWithStd(true)
      .setWithMean(true)

    // 对 brand_id 进行 StringIndexer 和 OneHotEncoder
    val indexer = new StringIndexer()
      .setInputCol("brand_id")
      .setOutputCol("brand_index")

    val encoder = new OneHotEncoder()
      .setInputCols(Array("brand_index"))
      .setOutputCols(Array("brand_vec"))
      .setDropLast(false) // 不丢弃最后一个，与题目描述要求(若属于该品牌为1，否则为0)保持一致

    val pipeline = new Pipeline().setStages(Array(assembler, scaler, indexer, encoder))
    val model = pipeline.fit(skuInfoDouble)
    val transformed = model.transform(skuInfoDouble)

    // 展开 scaled_features 和 brand_vec
    // 因为要求输出格式为： 1.0, 0.892346, 1.72568, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0
    val firstRow = transformed.orderBy("id").first()

    val idVal = firstRow.getAs[Number]("id").doubleValue()
    val scaledArr = firstRow.getAs[org.apache.spark.ml.linalg.Vector]("scaled_features").toArray
    val brandVecArr = firstRow.getAs[org.apache.spark.ml.linalg.SparseVector]("brand_vec").toArray

    // 组合前10列
    val combinedArr = Array(idVal) ++ scaledArr ++ brandVecArr
    val top10Cols = combinedArr.take(10)

    println("--------------------第一条数据前10列结果展示为：---------------------")
    println(top10Cols.mkString(","))

    spark.stop()
  }
}
