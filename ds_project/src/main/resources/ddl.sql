-- ClickHouse DDL
CREATE DATABASE IF NOT EXISTS shtd_result;

CREATE TABLE shtd_result.cityavgcmpprovince (
    cityname String,
    cityavgconsumption Float64,
    provincename String,
    provinceavgconsumption Float64,
    comparison String
) ENGINE = MergeTree()
ORDER BY cityname;

CREATE TABLE shtd_result.citymidcmpprovince (
    cityname String,
    citymidconsumption Float64,
    provincename String,
    provincemidconsumption Float64,
    comparison String
) ENGINE = MergeTree()
ORDER BY cityname;

CREATE TABLE shtd_result.regiontopthree (
    provincename String,
    citynames String,
    cityamount String
) ENGINE = MergeTree()
ORDER BY provincename;

CREATE TABLE shtd_result.topten (
    topquantityid Int32,
    topquantityname String,
    topquantity Int32,
    toppriceid String,
    toppricename String,
    topprice Decimal(18,2),
    sequence Int32
) ENGINE = MergeTree()
ORDER BY sequence;

CREATE TABLE shtd_result.userrepurchasedrate (
    purchaseduser Int32,
    repurchaseduser Int32,
    repurchaserate String
) ENGINE = MergeTree()
ORDER BY purchaseduser;

CREATE TABLE shtd_result.accumulateconsumption (
    consumptiontime String,
    consumptionadd Float64,
    consumptionacc Float64
) ENGINE = MergeTree()
ORDER BY consumptiontime;

CREATE TABLE shtd_result.slidewindowconsumption (
    consumptiontime String,
    consumptionsum Float64,
    consumptioncount Float64,
    consumptionavg Float64
) ENGINE = MergeTree()
ORDER BY consumptiontime;

CREATE TABLE shtd_result.orderpostiveaggr (
    sn Int32,
    orderprice Float64,
    orderdetailcount Int32
) ENGINE = MergeTree()
ORDER BY sn;
