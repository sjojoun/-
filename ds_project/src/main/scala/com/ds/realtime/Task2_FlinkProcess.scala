package com.ds.realtime

import java.util.Properties
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.api.common.functions.MapFunction
import com.alibaba.fastjson.{JSON, JSONObject}
import redis.clients.jedis.Jedis
import java.sql.{Connection, DriverManager, PreparedStatement}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import scala.collection.JavaConverters._

/**
 * 实时计算 - 任务二：使用Flink处理Kafka中的数据
 */
object Task2_FlinkProcess {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // Kafka properties
    val props = new Properties()
    props.setProperty("bootstrap.servers", "master:9092")
    props.setProperty("group.id", "flink_consumer_group")

    // Consumer for ods_mall_data (from Maxwell)
    val odsMallDataStream = env.addSource(new FlinkKafkaConsumer[String]("ods_mall_data", new SimpleStringSchema(), props))

    // Consumer for ods_mall_log (from Flume)
    val odsMallLogStream = env.addSource(new FlinkKafkaConsumer[String]("ods_mall_log", new SimpleStringSchema(), props))

    // ==========================================================
    // 1. 分发 ods_mall_data 至 DWD 层 Topic (fact_order_master, fact_order_detail)
    // Maxwell format: {"database":"ds_db01","table":"order_master","type":"insert","data":{"order_id":1,...}}
    // ==========================================================

    val dwdOrderMasterStream = odsMallDataStream.filter { jsonStr =>
      val jsonObj = JSON.parseObject(jsonStr)
      jsonObj.getString("table") == "order_master"
    }.map { jsonStr =>
      val jsonObj = JSON.parseObject(jsonStr)
      jsonObj.getString("data")
    }

    val dwdOrderDetailStream = odsMallDataStream.filter { jsonStr =>
      val jsonObj = JSON.parseObject(jsonStr)
      jsonObj.getString("table") == "order_detail"
    }.map { jsonStr =>
      val jsonObj = JSON.parseObject(jsonStr)
      jsonObj.getString("data")
    }

    // Producer properties
    val producerProps = new Properties()
    producerProps.setProperty("bootstrap.servers", "master:9092")

    dwdOrderMasterStream.addSink(new FlinkKafkaProducer[String]("fact_order_master", new SimpleStringSchema(), producerProps))
    dwdOrderDetailStream.addSink(new FlinkKafkaProducer[String]("fact_order_detail", new SimpleStringSchema(), producerProps))

    // ==========================================================
    // 2. 分发 ods_mall_log 至 DWD 层 Topic (dim_customer_login_log)
    // 假设日志格式包含表前缀
    // ==========================================================
    val dwdCustomerLogStream = odsMallLogStream.filter { logStr =>
      logStr.contains("customer_login_log")
    }.map { logStr => logStr } // 假设直接将包含关键字的行视为对应日志数据

    dwdCustomerLogStream.addSink(new FlinkKafkaProducer[String]("dim_customer_login_log", new SimpleStringSchema(), producerProps))

    // ==========================================================
    // 3. 备份数据至 HBase (order_master, order_detail, customer_login_log)
    // ==========================================================

    class HBaseSink(tableNameStr: String, family: String) extends RichSinkFunction[String] {
      var connection: org.apache.hadoop.hbase.client.Connection = _

      override def open(parameters: Configuration): Unit = {
        val conf = HBaseConfiguration.create()
        conf.set("hbase.zookeeper.quorum", "master:2181")
        connection = ConnectionFactory.createConnection(conf)
      }

      override def invoke(value: String, context: org.apache.flink.streaming.api.functions.sink.SinkFunction.Context): Unit = {
        val table = connection.getTable(TableName.valueOf(tableNameStr))

        // 解析 JSON 数据
        val jsonObj = JSON.parseObject(value)
        val rowKeyStr = scala.util.Random.nextInt(10).toString + new java.text.SimpleDateFormat("yyyyMMddHHmmssSSS").format(new java.util.Date())

        val put = new Put(Bytes.toBytes(rowKeyStr))

        // 遍历 JSON 的每个字段，写入 HBase
        val entries = jsonObj.entrySet().iterator()
        while (entries.hasNext) {
          val entry = entries.next()
          val colName = entry.getKey
          val colValue = entry.getValue.toString
          put.addColumn(Bytes.toBytes(family), Bytes.toBytes(colName), Bytes.toBytes(colValue))
        }

        table.put(put)
        table.close()
      }

      override def close(): Unit = {
        if (connection != null) connection.close()
      }
    }

    dwdOrderMasterStream.addSink(new HBaseSink("ods:order_master", "info"))
    dwdOrderDetailStream.addSink(new HBaseSink("ods:order_detail", "info"))
    // 日志如果不是JSON可能需要专门解析
    dwdCustomerLogStream.map(x => {
      val map = new java.util.HashMap[String, String]()
      val parts = x.split(",")
      for (i <- parts.indices) { map.put(s"col_$i", parts(i)) }
      com.alibaba.fastjson.JSON.toJSONString(map)
    }).addSink(new HBaseSink("ods:customer_login_log", "info"))

    // ==========================================================
    // 4. Redis Sink 统计实时订单数量 (totalcount)
    // ==========================================================
    val orderCountStream = dwdOrderMasterStream.map(_ => 1L).keyBy(_ => "total").sum(0)

    orderCountStream.addSink(new RichSinkFunction[Long] {
      var jedis: Jedis = _
      override def open(parameters: Configuration): Unit = { jedis = new Jedis("master", 6379) }
      override def invoke(value: Long, context: org.apache.flink.streaming.api.functions.sink.SinkFunction.Context): Unit = {
        jedis.set("totalcount", value.toString)
      }
      override def close(): Unit = { if (jedis != null) jedis.close() }
    })

    // ==========================================================
    // 5. 实时统计销售量前3的商品 (top3itemamount)
    // ==========================================================
    // 提取 product_id 和 product_cnt
    case class ProductCount(productId: Int, count: Int)

    val productCountStream = dwdOrderDetailStream.map { jsonStr =>
      val obj = JSON.parseObject(jsonStr)
      ProductCount(obj.getInteger("product_id"), obj.getInteger("product_cnt"))
    }.keyBy(_.productId).sum("count")

    // 这里简化处理：将流收集到一个算子中维护Top3
    productCountStream.keyBy(_ => "all").process(new org.apache.flink.streaming.api.functions.KeyedProcessFunction[String, ProductCount, String] {
      val mapState = new scala.collection.mutable.HashMap[Int, Int]()

      override def processElement(value: ProductCount, ctx: org.apache.flink.streaming.api.functions.KeyedProcessFunction[String, ProductCount, String]#Context, out: org.apache.flink.util.Collector[String]): Unit = {
        mapState.put(value.productId, value.count)
        val top3 = mapState.toSeq.sortBy(-_._2).take(3).map(x => s"${x._1}:${x._2}").mkString(",")
        out.collect(s"[$top3]")
      }
    }).addSink(new RichSinkFunction[String] {
      var jedis: Jedis = _
      override def open(parameters: Configuration): Unit = { jedis = new Jedis("master", 6379) }
      override def invoke(value: String, context: org.apache.flink.streaming.api.functions.sink.SinkFunction.Context): Unit = {
        jedis.set("top3itemamount", value)
      }
      override def close(): Unit = { if (jedis != null) jedis.close() }
    })

    // ==========================================================
    // 6. 实时统计销售额前3的商品 (top3itemconsumption)
    // ==========================================================
    case class ProductSales(productId: Int, sales: Double)

    val productSalesStream = dwdOrderDetailStream.map { jsonStr =>
      val obj = JSON.parseObject(jsonStr)
      // 销售额 = 单价 * 数量
      ProductSales(obj.getInteger("product_id"), obj.getDouble("product_price") * obj.getInteger("product_cnt"))
    }.keyBy(_.productId).sum("sales")

    productSalesStream.keyBy(_ => "all").process(new org.apache.flink.streaming.api.functions.KeyedProcessFunction[String, ProductSales, String] {
      val mapState = new scala.collection.mutable.HashMap[Int, Double]()

      override def processElement(value: ProductSales, ctx: org.apache.flink.streaming.api.functions.KeyedProcessFunction[String, ProductSales, String]#Context, out: org.apache.flink.util.Collector[String]): Unit = {
        mapState.put(value.productId, value.sales)
        val top3 = mapState.toSeq.sortBy(-_._2).take(3).map(x => s"${x._1}:${x._2}").mkString(",")
        out.collect(s"[$top3]")
      }
    }).addSink(new RichSinkFunction[String] {
      var jedis: Jedis = _
      override def open(parameters: Configuration): Unit = { jedis = new Jedis("master", 6379) }
      override def invoke(value: String, context: org.apache.flink.streaming.api.functions.sink.SinkFunction.Context): Unit = {
        jedis.set("top3itemconsumption", value)
      }
      override def close(): Unit = { if (jedis != null) jedis.close() }
    })

    // ==========================================================
    // 7. 监控已退款存入ClickHouse
    // ==========================================================
    val refundStream = dwdOrderMasterStream.filter { jsonStr =>
      val obj = JSON.parseObject(jsonStr)
      // 假设 order_status = 2 表示已退款
      obj.getInteger("order_status") == 2
    }

    class ClickHouseRefundSink extends RichSinkFunction[String] {
      var conn: Connection = _
      var pstmt: PreparedStatement = _

      override def open(parameters: Configuration): Unit = {
        Class.forName("ru.yandex.clickhouse.ClickHouseDriver")
        conn = DriverManager.getConnection("jdbc:clickhouse://master:8123/shtd_result")
        // 假设只需保存原JSON数据，这里仅作演示
        pstmt = conn.prepareStatement("INSERT INTO order_master (data) VALUES (?)")
      }

      override def invoke(value: String, context: org.apache.flink.streaming.api.functions.sink.SinkFunction.Context): Unit = {
        pstmt.setString(1, value)
        pstmt.executeUpdate()
      }

      override def close(): Unit = {
        if (pstmt != null) pstmt.close()
        if (conn != null) conn.close()
      }
    }

    refundStream.addSink(new ClickHouseRefundSink())

    // ==========================================================
    // 8. 实时统计消费额前2的用户 (top2userconsumption)
    // ==========================================================
    case class UserSales(userId: Int, userName: String, sales: Double)

    val userSalesStream = dwdOrderMasterStream.map { jsonStr =>
      val obj = JSON.parseObject(jsonStr)
      // 此处假设订单中包含 customer_name 或者能在 Flink 中获取。若需关联 HBase 可用 Async I/O
      // 简单起见，从 JSON 获取 customer_id 和 order_money，并给出默认名称 "张三" 模拟获取
      // User name can be joined dynamically from HBase via AsyncFunction or Broadcast State.
      // Since dimension table is huge, Async I/O pattern is ideal:
      // e.g., val enrichedStream = AsyncDataStream.unorderedWait(dwdOrderMasterStream, new AsyncHBaseLookup(), 10, TimeUnit.SECONDS)
      // Mocking string here for simplicity.
      UserSales(obj.getInteger("customer_id"), "用户" + obj.getInteger("customer_id"), obj.getDouble("order_money"))
    }.keyBy(_.userId).sum("sales")

    userSalesStream.keyBy(_ => "all").process(new org.apache.flink.streaming.api.functions.KeyedProcessFunction[String, UserSales, String] {
      val mapState = new scala.collection.mutable.HashMap[Int, (String, Double)]()

      override def processElement(value: UserSales, ctx: org.apache.flink.streaming.api.functions.KeyedProcessFunction[String, UserSales, String]#Context, out: org.apache.flink.util.Collector[String]): Unit = {
        mapState.put(value.userId, (value.userName, value.sales))
        val top2 = mapState.toSeq.sortBy(-_._2._2).take(2).map(x => s"${x._1}:${x._2._1}:${x._2._2}").mkString(",")
        out.collect(s"[$top2]")
      }
    }).addSink(new RichSinkFunction[String] {
      var jedis: Jedis = _
      override def open(parameters: Configuration): Unit = { jedis = new Jedis("master", 6379) }
      override def invoke(value: String, context: org.apache.flink.streaming.api.functions.sink.SinkFunction.Context): Unit = {
        jedis.set("top2userconsumption", value)
      }
      override def close(): Unit = { if (jedis != null) jedis.close() }
    })

    // ==========================================================
    // 9. 双流 JOIN (order_master & order_detail)
    // ==========================================================
    case class OrderMaster(orderId: Int, orderSn: Int, orderMoney: Double)
    case class OrderDetail(orderId: Int, productCnt: Int)

    val omStream = dwdOrderMasterStream.map { json =>
      val obj = JSON.parseObject(json)
      OrderMaster(obj.getInteger("order_id"), obj.getInteger("order_sn"), obj.getDouble("order_money"))
    }

    val odStream = dwdOrderDetailStream.map { json =>
      val obj = JSON.parseObject(json)
      OrderDetail(obj.getInteger("order_id"), obj.getInteger("product_cnt"))
    }

    // 题目建议使用滚动窗口
    val joinedStream = omStream.join(odStream)
      .where(_.orderId)
      .equalTo(_.orderId)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
      .apply { (om, od) => (om.orderSn, om.orderMoney, od.productCnt) }

    // 按照sn汇总商品数量
    val aggrStream = joinedStream.keyBy(_._1).reduce { (v1, v2) =>
      (v1._1, v1._2, v1._3 + v2._3)
    }

    class ClickHouseJoinSink extends RichSinkFunction[(Int, Double, Int)] {
      var conn: Connection = _
      var pstmt: PreparedStatement = _

      override def open(parameters: Configuration): Unit = {
        Class.forName("ru.yandex.clickhouse.ClickHouseDriver")
        conn = DriverManager.getConnection("jdbc:clickhouse://master:8123/shtd_result")
        pstmt = conn.prepareStatement("INSERT INTO orderpostiveaggr (sn, orderprice, orderdetailcount) VALUES (?, ?, ?)")
      }

      override def invoke(value: (Int, Double, Int), context: org.apache.flink.streaming.api.functions.sink.SinkFunction.Context): Unit = {
        pstmt.setInt(1, value._1)
        pstmt.setDouble(2, value._2)
        pstmt.setInt(3, value._3)
        pstmt.executeUpdate()
      }

      override def close(): Unit = {
        if (pstmt != null) pstmt.close()
        if (conn != null) conn.close()
      }
    }

    aggrStream.addSink(new ClickHouseJoinSink())

    env.execute("Task2_FlinkProcess")
  }
}
