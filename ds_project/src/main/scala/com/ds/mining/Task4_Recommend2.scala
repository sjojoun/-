package com.ds.mining

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.mllib.linalg.SingularValueDecomposition
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import scala.collection.mutable

/**
 * 数据挖掘 - 任务四：推荐系统（二）
 */
object Task4_Recommend2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Task4_Recommend2")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    val orderDetailDf = spark.table("dwd.fact_order_detail")

    // SVD分解推荐
    // 1. 构建用户-商品矩阵
    val userProducts = orderDetailDf.select("customer_id", "product_id").distinct()

    val targetUserId = 5811

    // 将用户ID映射为长整型索引（因为CoordinateMatrix需要长整型作为行列索引）
    val customerMapping = userProducts.select("customer_id").distinct()
      .withColumn("cust_idx", monotonically_increasing_id())

    val productMapping = userProducts.select("product_id").distinct()
      .withColumn("prod_idx", monotonically_increasing_id())

    val mappedDf = userProducts
      .join(customerMapping, Seq("customer_id"))
      .join(productMapping, Seq("product_id"))
      .select($"cust_idx", $"prod_idx")

    // RDD[(User, Product, Rating)]
    val ratings = mappedDf.rdd.map(r => MatrixEntry(r.getLong(0), r.getLong(1), 1.0))

    // 2. SVD分解，保留前95%信息量
    val mat = new CoordinateMatrix(ratings)
    val numUsers = mat.numRows()
    val numProducts = mat.numCols()

    val rowMat = mat.toRowMatrix()

    // 保留至少保留前95%的信息量，即保留特征值平方和达到总平方和95%的奇异值个数
    // 我们先取一个合理的 k 值，假设取前 100 维 (或商品数的某个比例)
    // 实际可以通过计算总方差来动态确定 k，此处由于集群大小与商品数量未知，我们固定取一个合理值比如 k = math.min(50, numProducts.toInt)
    val maxK = scala.math.min(100, numProducts.toInt)
    val initSvd = rowMat.computeSVD(maxK, computeU = false)
    val sArr = initSvd.s.toArray
    val totalVariance = sArr.map(x => x * x).sum
    var currentVariance = 0.0
    var k = 0
    while (k < sArr.length && currentVariance < 0.95 * totalVariance) {
      currentVariance += sArr(k) * sArr(k)
      k += 1
    }
    val svd: SingularValueDecomposition[org.apache.spark.mllib.linalg.RowMatrix, Matrix] = rowMat.computeSVD(k, computeU = true)

    val U: org.apache.spark.mllib.linalg.RowMatrix = svd.U
    val s: Vector = svd.s
    val V: Matrix = svd.V

    // V 中每行为一个商品在隐含特征空间的表示向量
    // 根据 targetUserId=5811 的已购买商品
    val targetUserIdxRow = customerMapping.filter($"customer_id" === targetUserId).select("cust_idx").collect()

    if (targetUserIdxRow.isEmpty) {
      println(s"User $targetUserId not found in purchase history.")
      spark.stop()
      return
    }

    val targetUserIdx = targetUserIdxRow(0).getLong(0)

    val targetProductsIdx = mappedDf.filter($"cust_idx" === targetUserIdx).select("prod_idx").collect().map(_.getLong(0)).toSet

    // 获取降维后的商品矩阵，为了计算相似度
    // V 是 d x k 矩阵，每一行对应一个商品，但是索引映射需要注意
    // RowMatrix 的 computeSVD 返回的 V 的行对应着原特征，即商品
    // 为了关联商品ID，将 V 转为数组
    val VArray = V.toArray
    val numColsV = V.numCols // k
    val numRowsV = V.numRows // numProducts

    val productFeaturesMap = (0 until numRowsV).map { r =>
      val vec = new Array[Double](numColsV)
      for (c <- 0 until numColsV) {
        vec(c) = VArray(c * numRowsV + r)
      }
      (r.toLong, Vectors.dense(vec))
    }.toMap

    // 相似度计算函数
    def cosineSimilarity(v1: Vector, v2: Vector): Double = {
      val dotProduct = org.apache.spark.mllib.linalg.BLAS.dot(v1, v2)
      val norm1 = Vectors.norm(v1, 2)
      val norm2 = Vectors.norm(v2, 2)
      if (norm1 == 0 || norm2 == 0) 0.0 else dotProduct / (norm1 * norm2)
    }

    val targetProductsFeatures = targetProductsIdx.filter(productFeaturesMap.contains).map(productFeaturesMap)

    val allProductIdx = productMapping.select("product_id", "prod_idx").collect()

    // 推荐未购买商品
    val results = allProductIdx.flatMap { row =>
      val pid = row.getInt(0)
      val pIdx = row.getLong(1)

      if (!targetProductsIdx.contains(pIdx) && productFeaturesMap.contains(pIdx)) {
        val pVec = productFeaturesMap(pIdx)
        // 累加求均值
        val sumSim = targetProductsFeatures.map(tv => cosineSimilarity(pVec, tv)).sum
        val avgSim = if (targetProductsFeatures.nonEmpty) sumSim / targetProductsFeatures.size else 0.0
        Some((pid, avgSim))
      } else {
        None
      }
    }.sortBy(-_._2).take(5)

    println("------------------------推荐Top5结果如下------------------------")
    results.zipWithIndex.foreach { case ((pid, sim), idx) =>
      println(s"相似度top${idx + 1}(商品id：$pid，平均相似度：$sim)")
    }

    spark.stop()
  }
}
