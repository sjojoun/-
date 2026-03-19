-- Hive DDL
CREATE DATABASE IF NOT EXISTS ods;
CREATE DATABASE IF NOT EXISTS dwd;
CREATE DATABASE IF NOT EXISTS dws;

-- Hive HBase External Tables
CREATE EXTERNAL TABLE IF NOT EXISTS ods.hbase_order_master (
    rowkey string,
    order_id int,
    order_sn int,
    order_money double
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,info:order_id,info:order_sn,info:order_money")
TBLPROPERTIES ("hbase.table.name" = "ods:order_master");

CREATE EXTERNAL TABLE IF NOT EXISTS ods.hbase_order_detail (
    rowkey string,
    order_id int,
    product_id int,
    product_cnt int,
    product_price double
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,info:order_id,info:product_id,info:product_cnt,info:product_price")
TBLPROPERTIES ("hbase.table.name" = "ods:order_detail");

CREATE EXTERNAL TABLE IF NOT EXISTS ods.hbase_customer_login_log (
    rowkey string,
    customer_id int,
    login_time string,
    login_ip string
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,info:col_1,info:col_2,info:col_3")
TBLPROPERTIES ("hbase.table.name" = "ods:customer_login_log");
