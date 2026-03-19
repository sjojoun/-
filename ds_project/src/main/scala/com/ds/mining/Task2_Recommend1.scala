package com.ds.mining

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.linalg.{Vector, Vectors}
import scala.collection.mutable

/**
 * 数据挖掘 - 任务二：推荐系统（一）
 */
object Task2_Recommend1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Task2_Recommend1")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    // 根据前一个任务获取的10位用户，获取其已购买过的商品并剔除用户5811已买的
    val targetUser = 5811
    val orderDetailDf = spark.table("dwd.fact_order_detail").cache()

    // 找出所有其他用户购买的商品，并与5811购买的商品进行交集统计获取前10用户
    val targetProductsDf = orderDetailDf.filter($"customer_id" === targetUser).select("product_id").distinct()

    val otherUsersProducts = orderDetailDf.filter($"customer_id" =!= targetUser)
      .select("customer_id", "product_id").distinct()
      .join(targetProductsDf, Seq("product_id"), "inner")
      .groupBy("customer_id")
      .agg(count("product_id").as("same_product_count"))
      .orderBy($"same_product_count".desc)
      .limit(10)

    val top10UsersDf = otherUsersProducts.select("customer_id")

    // 10位用户已购买商品
    val top10UserPurchases = orderDetailDf.join(top10UsersDf, Seq("customer_id"), "inner")
      .select("customer_id", "product_id")
      .distinct()

    // 剔除用户5811已购买的商品
    val targetProductsList = targetProductsDf.collect().map(_.getInt(0)).toSet

    val filteredPurchases = top10UserPurchases.filter(!$"product_id".isin(targetProductsList.toSeq: _*))

    // 获取候选商品 (这10个用户买过但5811没买过的)
    val candidateProducts = filteredPurchases.select("product_id").distinct().collect().map(_.getInt(0))

    // 为了计算商品间的余弦相似度，通常使用基于用户购买行为的向量表示。
    // 即每个商品表示为购买它的用户向量。或者计算用户的相似度加权。
    // 题目要求：通过计算这10位用户已购买的商品（剔除用户5811已购买的商品）与用户5811已购买的商品数据集中商品的余弦相似度累加再求均值，输出均值前5商品id作为推荐使用。

    // 1. 获取商品-用户共现矩阵 (此处简化，采用简单的基于购买记录的相似度，或基于商品特征的相似度)
    // 根据题意，需要计算候选商品 p_i 与 目标商品 p_j 之间的相似度
    // 为了计算商品之间的相似度，我们先构建商品向量。使用用户ID作为特征维度。

    val userProducts = orderDetailDf.select("customer_id", "product_id").distinct()

    // 将用户ID映射为索引
    val userIndexer = new org.apache.spark.ml.feature.StringIndexer()
      .setInputCol("customer_id")
      .setOutputCol("user_index")
    val indexedUsers = userIndexer.fit(userProducts).transform(userProducts)

    val numUsers = indexedUsers.select("user_index").distinct().count().toInt

    val productVectorsDf = indexedUsers.groupBy("product_id")
      .agg(collect_list("user_index").as("user_indices"))
      .map { row =>
        val pid = row.getInt(0)
        val indices = row.getAs[scala.collection.mutable.WrappedArray[Double]](1).toArray
        val sortedIndices = indices.distinct.sorted
        val values = Array.fill(sortedIndices.length)(1.0)
        val vec = Vectors.sparse(numUsers, sortedIndices.map(_.toInt), values)
        (pid, vec)
      }.toDF("product_id", "features")

    // 相似度计算函数
    def cosineSimilarity(v1: Vector, v2: Vector): Double = {
      val dotProduct = org.apache.spark.ml.linalg.BLAS.dot(v1, v2)
      val norm1 = Vectors.norm(v1, 2)
      val norm2 = Vectors.norm(v2, 2)
      if (norm1 == 0 || norm2 == 0) 0.0 else dotProduct / (norm1 * norm2)
    }

    val productFeatures = productVectorsDf.collect().map(r => (r.getInt(0), r.getAs[Vector](1))).toMap

    val targetFeatures = targetProductsList.filter(productFeatures.contains).map(productFeatures)

    val results = candidateProducts.map { pid =>
      if (productFeatures.contains(pid)) {
        val candidateVec = productFeatures(pid)
        // 累加计算相似度并求均值
        val sumSim = targetFeatures.map(tv => cosineSimilarity(candidateVec, tv)).sum
        val avgSim = if (targetFeatures.nonEmpty) sumSim / targetFeatures.size else 0.0
        (pid, avgSim)
      } else {
        (pid, 0.0)
      }
    }.sortBy(-_._2).take(5)

    println("------------------------推荐Top5结果如下------------------------")
    results.zipWithIndex.foreach { case ((pid, sim), idx) =>
      println(s"相似度top${idx + 1}(商品id：$pid，平均相似度：$sim)")
    }

    spark.stop()
  }
}
