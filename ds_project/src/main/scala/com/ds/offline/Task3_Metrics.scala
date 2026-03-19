package com.ds.offline

import org.apache.spark.sql.{SaveMode, SparkSession, DataFrame}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import java.util.Properties

/**
 * 离线数据处理 - 任务三：指标计算
 */
object Task3_Metrics {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Task3_Metrics")
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("CREATE DATABASE IF NOT EXISTS dws")

    val chUrl = "jdbc:clickhouse://master:8123/shtd_result"
    val chProps = new Properties()
    // chProps.put("user", "default")
    // chProps.put("password", "")

    import spark.implicits._

    // 假设 order_master 表在 dwd 中名为 fact_order_master
    val orderMasterDf = spark.table("dwd.fact_order_master")
    val orderDetailDf = spark.table("dwd.fact_order_detail")
    val customerDimDf = spark.table("dwd.dim_customer_inf")

    // 过滤掉已退款或已取消的订单（此处需要根据实际表结构判断字段，例如 order_status = '退款' 或 '取消'，这里以 order_status NOT IN 为例）
    // 实际需根据业务，这里暂且假设 0 正常，1 取消，2 退款
    val validOrders = orderMasterDf.filter($"order_status" =!= 1 && $"order_status" =!= 2)

    // --- 1. user_consumption_day_aggr ---
    // 每人每天下单数量和总金额
    val userConsumptionDay = validOrders.join(customerDimDf, Seq("customer_id"), "left")
      .withColumn("year", year($"create_time"))
      .withColumn("month", month($"create_time"))
      .withColumn("day", dayofmonth($"create_time"))
      .groupBy("customer_id", "customer_name", "year", "month", "day")
      .agg(
        sum("order_money").as("total_amount"),
        count("order_id").as("total_count")
      )

    userConsumptionDay.write.mode(SaveMode.Overwrite)
      .partitionBy("year", "month", "day")
      .format("hive")
      .saveAsTable("dws.user_consumption_day_aggr")

    // --- 2. city_consumption_day_aggr ---
    // 每个城市每月下单数量和总金额，并生成sequence排名
    val cityConsumptionMonth = validOrders
      .withColumn("year", year($"create_time"))
      .withColumn("month", month($"create_time"))
      .groupBy("city", "province", "year", "month")
      .agg(
        sum("order_money").as("total_amount"),
        count("order_id").as("total_count")
      )
      .withColumnRenamed("city", "city_name")
      .withColumnRenamed("province", "province_name")

    val windowSpecCity = Window.partitionBy("province_name", "year", "month").orderBy($"total_amount".desc)
    val citySeqDf = cityConsumptionMonth.withColumn("sequence", row_number().over(windowSpecCity))

    citySeqDf.write.mode(SaveMode.Overwrite)
      .partitionBy("year", "month")
      .format("hive")
      .saveAsTable("dws.city_consumption_day_aggr")

    // --- 3. cityavgcmpprovince (ClickHouse) ---
    val cityMonthAvg = validOrders.groupBy("city", "province").agg(avg("order_money").as("cityavgconsumption"))
    val provMonthAvg = validOrders.groupBy("province").agg(avg("order_money").as("provinceavgconsumption"))

    val cityAvgCmp = cityMonthAvg.join(provMonthAvg, Seq("province"))
      .withColumn("comparison",
        when($"cityavgconsumption" > $"provinceavgconsumption", "高")
        .when($"cityavgconsumption" < $"provinceavgconsumption", "低")
        .otherwise("相同")
      )
      .select(
        $"city".as("cityname"),
        $"cityavgconsumption",
        $"province".as("provincename"),
        $"provinceavgconsumption",
        $"comparison"
      )

    cityAvgCmp.write.mode(SaveMode.Append).jdbc(chUrl, "cityavgcmpprovince", chProps)

    // --- 4. citymidcmpprovince (ClickHouse) ---
    // Spark 计算中位数使用 percentile_approx
    val cityMonthMid = validOrders.groupBy("city", "province").agg(expr("percentile_approx(order_money, 0.5)").as("citymidconsumption"))
    val provMonthMid = validOrders.groupBy("province").agg(expr("percentile_approx(order_money, 0.5)").as("provincemidconsumption"))

    val cityMidCmp = cityMonthMid.join(provMonthMid, Seq("province"))
      .withColumn("comparison",
        when($"citymidconsumption" > $"provincemidconsumption", "高")
        .when($"citymidconsumption" < $"provincemidconsumption", "低")
        .otherwise("相同")
      )
      .select(
        $"city".as("cityname"),
        $"citymidconsumption",
        $"province".as("provincename"),
        $"provincemidconsumption",
        $"comparison"
      )

    cityMidCmp.write.mode(SaveMode.Append).jdbc(chUrl, "citymidcmpprovince", chProps)

    // --- 5. regiontopthree (ClickHouse) ---
    // 每个省份2022年订单金额前3城市
    val regionTop = validOrders.filter(year($"create_time") === 2022)
      .groupBy("province", "city")
      .agg(sum("order_money").as("city_total"))

    val winRegion = Window.partitionBy("province").orderBy($"city_total".desc)
    val regionTop3 = regionTop.withColumn("rn", row_number().over(winRegion)).filter($"rn" <= 3)

    val regionAgg = regionTop3.groupBy("province").agg(
      concat_ws(",", collect_list("city")).as("citynames"),
      concat_ws(",", collect_list(round($"city_total", 0).cast("int"))).as("cityamount")
    ).withColumnRenamed("province", "provincename")

    regionAgg.write.mode(SaveMode.Append).jdbc(chUrl, "regiontopthree", chProps)

    // --- 6. topten (ClickHouse) ---
    // 销量前10商品，销售额前10商品
    // 这题需要按商品ID和商品名称统计。假设 detail 表有 product_id, product_name, product_cnt, product_price 等
    val productStats = orderDetailDf.groupBy("product_id", "product_name")
      .agg(
        sum("product_cnt").as("total_qty"),
        sum(expr("product_cnt * product_price")).as("total_sales")
      )

    val topQty = productStats.orderBy($"total_qty".desc).limit(10)
      .withColumn("sequence", row_number().over(Window.orderBy($"total_qty".desc)))
      .select($"product_id".as("topquantityid"), $"product_name".as("topquantityname"), $"total_qty".as("topquantity"), $"sequence")

    val topPrice = productStats.orderBy($"total_sales".desc).limit(10)
      .withColumn("sequence", row_number().over(Window.orderBy($"total_sales".desc)))
      .select($"product_id".as("toppriceid"), $"product_name".as("toppricename"), $"total_sales".as("topprice"), $"sequence")

    val topTen = topQty.join(topPrice, Seq("sequence"))
    topTen.write.mode(SaveMode.Append).jdbc(chUrl, "topten", chProps)

    // --- 7. userrepurchasedrate (ClickHouse) ---
    // 连续两天下单用户占比
    val userDays = validOrders.select("customer_id", "create_time")
      .withColumn("dt", to_date($"create_time"))
      .dropDuplicates("customer_id", "dt")

    val userDaysLag = userDays.withColumn("prev_dt", lag("dt", 1).over(Window.partitionBy("customer_id").orderBy("dt")))
    val repurchasedUsers = userDaysLag.filter(datediff($"dt", $"prev_dt") === 1).select("customer_id").distinct().count()
    val totalUsers = userDays.select("customer_id").distinct().count()

    val rate = if (totalUsers == 0) 0.0 else (repurchasedUsers.toDouble / totalUsers) * 100
    val formattedRate = f"${rate}%.1f%%"

    val rateDf = spark.createDataFrame(Seq((totalUsers, repurchasedUsers, formattedRate)))
      .toDF("purchaseduser", "repurchaseduser", "repurchaserate")

    rateDf.write.mode(SaveMode.Append).jdbc(chUrl, "userrepurchasedrate", chProps)

    // --- 8. 省份累计订单量 ---
    val provinceOrders = validOrders.groupBy("province").agg(count("order_id").as("Amount")).orderBy($"Amount".desc)

    // 行转列
    val orderedProvinces = provinceOrders.select("province").collect().map(_.getString(0)).toSeq
    val pivotProv = provinceOrders.groupBy().pivot("province", orderedProvinces).sum("Amount")
    pivotProv.show()

    // --- 9. accumulateconsumption (ClickHouse) ---
    // 2022-04-26 00:00:00 到 09:59:59，每小时新增订单与当天累加
    val startTime = "2022-04-26 00:00:00"
    val endTime = "2022-04-26 09:59:59"
    val accOrders = validOrders.filter($"create_time" >= lit(startTime) && $"create_time" <= lit(endTime))
      .withColumn("hour", date_format($"create_time", "yyyy-MM-dd HH"))

    val hourStats = accOrders.groupBy("hour").agg(sum("order_money").as("consumptionadd"))
    val winAcc = Window.orderBy("hour")
    val accResult = hourStats.withColumn("consumptionacc", sum("consumptionadd").over(winAcc))
      .withColumnRenamed("hour", "consumptiontime")

    accResult.write.mode(SaveMode.Append).jdbc(chUrl, "accumulateconsumption", chProps)

    // --- 10. slidewindowconsumption (ClickHouse) ---
    // 5小时窗口，1小时步长
    // Spark Structured Streaming 可以用 window，但离线可以手动 join 或者通过 range join
    // 此处简化处理：将时间截断为小时级别，转换成 unix timestamp
    val slideOrders = validOrders.filter($"create_time" >= lit(startTime) && $"create_time" <= lit(endTime))
      .withColumn("hour_ts", unix_timestamp(date_format($"create_time", "yyyy-MM-dd HH:00:00"), "yyyy-MM-dd HH:mm:ss"))
      .groupBy("hour_ts").agg(sum("order_money").as("sum_amt"), count("order_id").as("cnt_amt"))

    // 使用 spark sql 开窗 (基于 RANGE 或 JOIN 方式构建窗口)
    slideOrders.createOrReplaceTempView("hourly_stats")

    val slideSql = """
      SELECT
        from_unixtime(t1.hour_ts, 'yyyy-MM-dd HH') as consumptiontime,
        SUM(t2.sum_amt) as consumptionsum,
        SUM(t2.cnt_amt) as consumptioncount
      FROM hourly_stats t1
      JOIN hourly_stats t2
        ON t2.hour_ts BETWEEN t1.hour_ts - 4*3600 AND t1.hour_ts
      GROUP BY t1.hour_ts
      HAVING COUNT(DISTINCT t2.hour_ts) = 5  -- 时间不满5小时不触发计算
      ORDER BY t1.hour_ts
    """

    val slideResult = spark.sql(slideSql)
      .withColumn("consumptionavg", round($"consumptionsum" / $"consumptioncount", 2))

    slideResult.write.mode(SaveMode.Append).jdbc(chUrl, "slidewindowconsumption", chProps)

    spark.stop()
  }
}
