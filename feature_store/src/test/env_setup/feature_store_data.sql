CREATE SCHEMA IF NOT EXISTS retail_ext;
DROP TABLE IF EXISTS retail_ext.customer;
CREATE EXTERNAL TABLE retail_ext.customer
(
    CUSTOMERID INTEGER,
    CUSTOMERNAME VARCHAR(100),
    COUNTRY VARCHAR(100)
)
 STORED AS PARQUET
 LOCATION 's3a://splice-demo/kaggle-retail-tiny/customer';
DROP TABLE IF EXISTS retail_ext.product;
CREATE EXTERNAL TABLE retail_ext.product
(
    STOCKCODE VARCHAR(20),
    DESCRIPTION VARCHAR(200),
    MSRP DOUBLE
)
 STORED AS PARQUET
 LOCATION 's3a://splice-demo/kaggle-retail-tiny/product';
DROP TABLE IF EXISTS retail_ext.order_txn;
CREATE EXTERNAL TABLE retail_ext.order_txn
(
    INVOICE INTEGER,
    CUSTOMERID INTEGER,
    INVOICEDATE TIMESTAMP,
    ORDERCOUNTRY VARCHAR(100),
    CANCELLED BOOLEAN
)
 STORED AS PARQUET
 LOCATION 's3a://splice-demo/kaggle-retail-tiny/order';
DROP TABLE IF EXISTS retail_ext.order_line;
CREATE EXTERNAL TABLE retail_ext.order_line
(
    INVOICE INT,
    STOCKCODE VARCHAR(20),
    QUANTITY INTEGER,
    PRICE DOUBLE
)
 STORED AS PARQUET
 LOCATION 's3a://splice-demo/kaggle-retail-tiny/order_line';
DROP TABLE IF EXISTS retail_ext.product_categories;
CREATE EXTERNAL TABLE retail_ext.product_categories
(
    STOCKCODE VARCHAR(20),
    CAMPING_FLAG BOOLEAN,
    CLOTHING_FLAG BOOLEAN,
    DELICATESSEN_FLAG BOOLEAN,
    GARDEN_FLAG BOOLEAN,
    HOME_FLAG BOOLEAN,
    HOME_DECOR_FLAG BOOLEAN,
    JEWELRY_FLAG BOOLEAN,
    KITCHEN_FLAG BOOLEAN,
    LADIES_FLAG BOOLEAN,
    NOVELTY_FLAG BOOLEAN,
    SCHOOL_SUPPLIES_FLAG BOOLEAN,
    TOYS_FLAG BOOLEAN,
    TRAVEL_FLAG BOOLEAN,
    MISCELANEOUS_FLAG BOOLEAN
)
 STORED AS PARQUET
 LOCATION 's3a://splice-demo/kaggle-retail-tiny/product_categories';
CREATE SCHEMA IF NOT EXISTS retail;
DROP TABLE IF EXISTS retail.customer;
CREATE TABLE retail.customer
(
    CUSTOMERID INTEGER PRIMARY KEY,
    CUSTOMERNAME VARCHAR(100),
    COUNTRY VARCHAR(100)
);
DROP TABLE IF EXISTS retail.product;
CREATE TABLE retail.product
(
    STOCKCODE VARCHAR(20) PRIMARY KEY,
    DESCRIPTION VARCHAR(200),
    MSRP DOUBLE
)
;
DROP TABLE IF EXISTS retail.product_categories;
CREATE TABLE retail.product_categories
(
    STOCKCODE VARCHAR(20) PRIMARY KEY,
    CAMPING_FLAG BOOLEAN,
    CLOTHING_FLAG BOOLEAN,
    DELICATESSEN_FLAG BOOLEAN,
    GARDEN_FLAG BOOLEAN,
    HOME_FLAG BOOLEAN,
    HOME_DECOR_FLAG BOOLEAN,
    JEWELRY_FLAG BOOLEAN,
    KITCHEN_FLAG BOOLEAN,
    LADIES_FLAG BOOLEAN,
    NOVELTY_FLAG BOOLEAN,
    SCHOOL_SUPPLIES_FLAG BOOLEAN,
    TOYS_FLAG BOOLEAN,
    TRAVEL_FLAG BOOLEAN,
    MISCELANEOUS_FLAG BOOLEAN
)
;
DROP TABLE IF EXISTS retail.order_txn;
CREATE TABLE retail.order_txn
(
    INVOICE INTEGER PRIMARY KEY,
    CUSTOMERID INTEGER,
    INVOICEDATE TIMESTAMP,
    ORDERCOUNTRY VARCHAR(100),
    CANCELLED BOOLEAN
)
;
DROP TABLE IF EXISTS retail.order_line;
CREATE TABLE retail.order_line
(
    INVOICE INTEGER,
    STOCKCODE VARCHAR(20),
    QUANTITY INTEGER,
    PRICE DOUBLE,
    PRIMARY KEY(INVOICE, STOCKCODE)
);
INSERT INTO retail.customer SELECT * FROM retail_ext.customer;
INSERT INTO retail.product SELECT * FROM retail_ext.product;
INSERT INTO retail.product_categories SELECT * FROM retail_ext.product_categories;
INSERT INTO retail.order_line SELECT * FROM retail_ext.order_line;
INSERT INTO retail.order_txn
SELECT INVOICE, CUSTOMERID,
    TIMESTAMPADD(SQL_TSI_DAY, TIMESTAMPDIFF(SQL_TSI_DAY, timestamp('2011-12-01 00:00:00'), CURRENT_TIMESTAMP), INVOICEDATE) INVOICEDATE,
    ORDERCOUNTRY,
    CANCELLED
FROM retail_ext.order_txn
WHERE INVOICEDATE < timestamp('2011-12-01 00:00:00')
;
ANALYZE SCHEMA retail;
CREATE SCHEMA IF NOT EXISTS RETAIL_RFM;
DROP TABLE IF EXISTS RETAIL_RFM.CUSTOMER_CATEGORY_ACTIVITY;
CREATE TABLE RETAIL_RFM.CUSTOMER_CATEGORY_ACTIVITY
(
    CUSTOMERID INTEGER,
    INVOICEDATE DATE,
    CLOTHING_QTY INTEGER,
    DELICATESSEN_QTY INTEGER,
    GARDEN_QTY INTEGER,
    HOME_QTY INTEGER,
    HOME_DECOR_QTY INTEGER,
    JEWELRY_QTY INTEGER,
    KITCHEN_QTY INTEGER,
    LADIES_QTY INTEGER,
    NOVELTY_QTY INTEGER,
    SCHOOL_SUPPLIES_QTY INTEGER,
    TOYS_QTY INTEGER,
    TRAVEL_QTY INTEGER,
    TOTAL_QTY INTEGER,
    CLOTHING_REVENUE DECIMAL(10,2),
    DELICATESSEN_REVENUE DECIMAL(10,2),
    GARDEN_REVENUE DECIMAL(10,2),
    HOME_REVENUE DECIMAL(10,2),
    HOME_DECOR_REVENUE DECIMAL(10,2),
    JEWELRY_REVENUE DECIMAL(10,2),
    KITCHEN_REVENUE DECIMAL(10,2),
    LADIES_REVENUE DECIMAL(10,2),
    NOVELTY_REVENUE DECIMAL(10,2),
    SCHOOL_SUPPLIES_REVENUE DECIMAL(10,2),
    TOYS_REVENUE DECIMAL(10,2),
    TRAVEL_REVENUE DECIMAL(10,2),
    TOTAL_REVENUE DECIMAL(10,2),
    PRIMARY KEY (CUSTOMERID, INVOICEDATE)
);
INSERT INTO RETAIL_RFM.CUSTOMER_CATEGORY_ACTIVITY
SELECT o.customerid,
    CAST(INVOICEDATE AS DATE) INVOICEDATE,
        SUM(CASE WHEN CLOTHING_FLAG THEN  QUANTITY END ) CLOTHING_QTY,
        SUM(CASE WHEN DELICATESSEN_FLAG THEN  QUANTITY END ) DELICATESSEN_QTY,
        SUM(CASE WHEN GARDEN_FLAG THEN  QUANTITY END ) GARDEN_QTY,
        SUM(CASE WHEN HOME_FLAG THEN  QUANTITY END ) HOME_QTY,
        SUM(CASE WHEN HOME_DECOR_FLAG THEN  QUANTITY END ) HOME_DECOR_QTY,
        SUM(CASE WHEN JEWELRY_FLAG THEN  QUANTITY END ) JEWELRY_QTY,
        SUM(CASE WHEN KITCHEN_FLAG THEN  QUANTITY END ) KITCHEN_QTY,
        SUM(CASE WHEN LADIES_FLAG THEN  QUANTITY END ) LADIES_QTY,
        SUM(CASE WHEN NOVELTY_FLAG THEN  QUANTITY END ) NOVELTY_QTY,
        SUM(CASE WHEN SCHOOL_SUPPLIES_FLAG THEN  QUANTITY END ) SCHOOL_SUPPLIES_QTY,
        SUM(CASE WHEN TOYS_FLAG THEN  QUANTITY END ) TOYS_QTY,
        SUM(CASE WHEN TRAVEL_FLAG THEN  QUANTITY END ) TRAVEL_QTY,
        SUM( QUANTITY) TOTAL_QTY,
        SUM(CASE WHEN CLOTHING_FLAG THEN  QUANTITY * PRICE END ) CLOTHING_REVENUE,
        SUM(CASE WHEN DELICATESSEN_FLAG THEN  QUANTITY * PRICE END ) DELICATESSEN_REVENUE,
        SUM(CASE WHEN GARDEN_FLAG THEN  QUANTITY * PRICE END ) GARDEN_REVENUE,
        SUM(CASE WHEN HOME_FLAG THEN  QUANTITY * PRICE END ) HOME_REVENUE,
        SUM(CASE WHEN HOME_DECOR_FLAG THEN  QUANTITY * PRICE END ) HOME_DECOR_REVENUE,
        SUM(CASE WHEN JEWELRY_FLAG THEN  QUANTITY * PRICE END ) JEWELRY_REVENUE,
        SUM(CASE WHEN KITCHEN_FLAG THEN  QUANTITY * PRICE END ) KITCHEN_REVENUE,
        SUM(CASE WHEN LADIES_FLAG THEN  QUANTITY * PRICE END ) LADIES_REVENUE,
        SUM(CASE WHEN NOVELTY_FLAG THEN  QUANTITY * PRICE END ) NOVELTY_REVENUE,
        SUM(CASE WHEN SCHOOL_SUPPLIES_FLAG THEN  QUANTITY * PRICE END ) SCHOOL_SUPPLIES_REVENUE,
        SUM(CASE WHEN TOYS_FLAG THEN  QUANTITY * PRICE END ) TOYS_REVENUE,
        SUM(CASE WHEN TRAVEL_FLAG THEN  QUANTITY * PRICE END ) TRAVEL_REVENUE,
        SUM(QUANTITY * PRICE) TOTAL_REVENUE
FROM retail.order_txn o
INNER JOIN retail.order_line ol USING(INVOICE)
INNER JOIN retail.product_categories px USING(STOCKCODE)
WHERE CUSTOMERID IS NOT NULL -- only transactions with identified customers
GROUP BY 1,2
;
DROP TABLE IF EXISTS retail_rfm.calendar;
CREATE TABLE retail_rfm.calendar
(
    TheDate DATE,
    WeekNumOfYear INTEGER,
    DayOfWeek INTEGER
);
INSERT INTO retail_rfm.calendar (TheDate, WeekNumOfYear, DayOfWeek)
SELECT CURRENT_DATE - daynum, WEEK(CURRENT_DATE-daynum), EXTRACT(WEEKDAY FROM CURRENT_DATE-daynum)
FROM
( SELECT x.n*100+y.n*10+z.n as calc_num FROM (VALUES  0,1,2,3,4,5,6,7,8,9) x(n),  (VALUES  0,1,2,3,4,5,6,7,8,9) y(n),  (VALUES  0,1,2,3,4,5,6,7,8,9) z(n) ) xx(daynum)
;
DROP TABLE IF EXISTS retail_rfm.weeks;
CREATE TABLE retail_rfm.weeks
(
    week_end_date DATE
);
INSERT INTO retail_rfm.weeks SELECT TheDate FROM retail_rfm.calendar WHERE DayOfWeek = 7;
ANALYZE SCHEMA RETAIL_RFM;