INSERT INTO retail_fs.customer_lifetime
( CUSTOMERID, LAST_UPDATE_TS, CUSTOMER_LIFETIME_ACTIVE_DAYS,CUSTOMER_LIFETIME_QTY,
   CUSTOMER_LIFETIME_ITEMS_PER_ACTIVE_DAY,CUSTOMER_LIFETIME_REVENUE_PER_ACTIVE_DAY,
 CUSTOMER_LIFETIME_DAYS,CUSTOMER_DAYS_SINCE_PURCHASE, CUSTOMER_LIFETIME_VALUE,
 CUSTOMER_START_DATE)
SELECT CUSTOMERID, CAST(MAX(INVOICEDATE) AS TIMESTAMP) LAST_UPDATE_TS, -- use most recent event time for LAST_UPDATE_TS
    count(*) ACTIVE_DAYS,
    sum(TOTAL_QTY) LIFETIME_QTY,
    sum(TOTAL_QTY)*1.0/count(*) LIFETIME_ITEMS_PER_ACTIVE_DAY,
    sum(TOTAL_REVENUE)*1.0/count(*) LIFETIME_REVENUE_PER_ACTIVE_DAY,
    CURRENT_DATE - min(invoicedate) LIFETIME_DAYS,
    CURRENT_DATE - max(invoicedate) DAYS_SINCE_PURCHASE,
    sum(TOTAL_REVENUE) LIFETIME_VALUE,
    min(invoicedate) CUSTOMER_START_DATE
FROM RETAIL_RFM.CUSTOMER_CATEGORY_ACTIVITY group by 1;



-- create current RFM values based on most recently completed week for new customers
INSERT INTO RETAIL_FS.CUSTOMER_RFM_BY_CATEGORY
(
        CUSTOMERID, 
        LAST_UPDATE_TS, 
        CUSTOMER_RFM_CLOTHING_RATE_1W, CUSTOMER_RFM_CLOTHING_RATE_2W, CUSTOMER_RFM_CLOTHING_RATE_4W, CUSTOMER_RFM_CLOTHING_RATE_8W, CUSTOMER_RFM_CLOTHING_RATE_16W, CUSTOMER_RFM_CLOTHING_RATE_32W, CUSTOMER_RFM_CLOTHING_RATE_52W,
        CUSTOMER_RFM_DELICATESSEN_RATE_1W, CUSTOMER_RFM_DELICATESSEN_RATE_2W, CUSTOMER_RFM_DELICATESSEN_RATE_4W, CUSTOMER_RFM_DELICATESSEN_RATE_8W, CUSTOMER_RFM_DELICATESSEN_RATE_16W, CUSTOMER_RFM_DELICATESSEN_RATE_32W, CUSTOMER_RFM_DELICATESSEN_RATE_52W,
        CUSTOMER_RFM_GARDEN_RATE_1W, CUSTOMER_RFM_GARDEN_RATE_2W, CUSTOMER_RFM_GARDEN_RATE_4W, CUSTOMER_RFM_GARDEN_RATE_8W, CUSTOMER_RFM_GARDEN_RATE_16W, CUSTOMER_RFM_GARDEN_RATE_32W, CUSTOMER_RFM_GARDEN_RATE_52W,           
        CUSTOMER_RFM_HOME_RATE_1W, CUSTOMER_RFM_HOME_RATE_2W, CUSTOMER_RFM_HOME_RATE_4W, CUSTOMER_RFM_HOME_RATE_8W, CUSTOMER_RFM_HOME_RATE_16W, CUSTOMER_RFM_HOME_RATE_32W, CUSTOMER_RFM_HOME_RATE_52W,
        CUSTOMER_RFM_HOME_DECOR_RATE_1W, CUSTOMER_RFM_HOME_DECOR_RATE_2W, CUSTOMER_RFM_HOME_DECOR_RATE_4W, CUSTOMER_RFM_HOME_DECOR_RATE_8W, CUSTOMER_RFM_HOME_DECOR_RATE_16W, CUSTOMER_RFM_HOME_DECOR_RATE_32W, CUSTOMER_RFM_HOME_DECOR_RATE_52W,
        CUSTOMER_RFM_JEWELRY_RATE_1W, CUSTOMER_RFM_JEWELRY_RATE_2W, CUSTOMER_RFM_JEWELRY_RATE_4W, CUSTOMER_RFM_JEWELRY_RATE_8W, CUSTOMER_RFM_JEWELRY_RATE_16W, CUSTOMER_RFM_JEWELRY_RATE_32W, CUSTOMER_RFM_JEWELRY_RATE_52W,
        CUSTOMER_RFM_KITCHEN_RATE_1W, CUSTOMER_RFM_KITCHEN_RATE_2W, CUSTOMER_RFM_KITCHEN_RATE_4W, CUSTOMER_RFM_KITCHEN_RATE_8W, CUSTOMER_RFM_KITCHEN_RATE_16W, CUSTOMER_RFM_KITCHEN_RATE_32W, CUSTOMER_RFM_KITCHEN_RATE_52W,
        CUSTOMER_RFM_NOVELTY_RATE_1W, CUSTOMER_RFM_NOVELTY_RATE_2W, CUSTOMER_RFM_NOVELTY_RATE_4W, CUSTOMER_RFM_NOVELTY_RATE_8W, CUSTOMER_RFM_NOVELTY_RATE_16W, CUSTOMER_RFM_NOVELTY_RATE_32W, CUSTOMER_RFM_NOVELTY_RATE_52W,
        CUSTOMER_RFM_SCHOOL_SUPPLIES_RATE_1W, CUSTOMER_RFM_SCHOOL_SUPPLIES_RATE_2W, CUSTOMER_RFM_SCHOOL_SUPPLIES_RATE_4W, CUSTOMER_RFM_SCHOOL_SUPPLIES_RATE_8W, CUSTOMER_RFM_SCHOOL_SUPPLIES_RATE_16W, CUSTOMER_RFM_SCHOOL_SUPPLIES_RATE_32W, CUSTOMER_RFM_SCHOOL_SUPPLIES_RATE_52W,
        CUSTOMER_RFM_TOYS_RATE_1W, CUSTOMER_RFM_TOYS_RATE_2W, CUSTOMER_RFM_TOYS_RATE_4W, CUSTOMER_RFM_TOYS_RATE_8W, CUSTOMER_RFM_TOYS_RATE_16W, CUSTOMER_RFM_TOYS_RATE_32W, CUSTOMER_RFM_TOYS_RATE_52W,
        CUSTOMER_RFM_TRAVEL_RATE_1W, CUSTOMER_RFM_TRAVEL_RATE_2W, CUSTOMER_RFM_TRAVEL_RATE_4W, CUSTOMER_RFM_TRAVEL_RATE_8W, CUSTOMER_RFM_TRAVEL_RATE_16W, CUSTOMER_RFM_TRAVEL_RATE_32W, CUSTOMER_RFM_TRAVEL_RATE_52W,
        CUSTOMER_RFM_TOTAL_RATE_1W, CUSTOMER_RFM_TOTAL_RATE_2W, CUSTOMER_RFM_TOTAL_RATE_4W, CUSTOMER_RFM_TOTAL_RATE_8W, CUSTOMER_RFM_TOTAL_RATE_16W, CUSTOMER_RFM_TOTAL_RATE_32W, CUSTOMER_RFM_TOTAL_RATE_52W,
    
        CUSTOMER_RFM_CLOTHING_REVN_RATE_1W, CUSTOMER_RFM_CLOTHING_REVN_RATE_2W, CUSTOMER_RFM_CLOTHING_REVN_RATE_4W, CUSTOMER_RFM_CLOTHING_REVN_RATE_8W, CUSTOMER_RFM_CLOTHING_REVN_RATE_16W, CUSTOMER_RFM_CLOTHING_REVN_RATE_32W, CUSTOMER_RFM_CLOTHING_REVN_RATE_52W,
        CUSTOMER_RFM_DELICATESSEN_REVN_RATE_1W, CUSTOMER_RFM_DELICATESSEN_REVN_RATE_2W, CUSTOMER_RFM_DELICATESSEN_REVN_RATE_4W, CUSTOMER_RFM_DELICATESSEN_REVN_RATE_8W, CUSTOMER_RFM_DELICATESSEN_REVN_RATE_16W, CUSTOMER_RFM_DELICATESSEN_REVN_RATE_32W, CUSTOMER_RFM_DELICATESSEN_REVN_RATE_52W,
        CUSTOMER_RFM_GARDEN_REVN_RATE_1W, CUSTOMER_RFM_GARDEN_REVN_RATE_2W, CUSTOMER_RFM_GARDEN_REVN_RATE_4W, CUSTOMER_RFM_GARDEN_REVN_RATE_8W, CUSTOMER_RFM_GARDEN_REVN_RATE_16W, CUSTOMER_RFM_GARDEN_REVN_RATE_32W, CUSTOMER_RFM_GARDEN_REVN_RATE_52W,           
        CUSTOMER_RFM_HOME_REVN_RATE_1W, CUSTOMER_RFM_HOME_REVN_RATE_2W, CUSTOMER_RFM_HOME_REVN_RATE_4W, CUSTOMER_RFM_HOME_REVN_RATE_8W, CUSTOMER_RFM_HOME_REVN_RATE_16W, CUSTOMER_RFM_HOME_REVN_RATE_32W, CUSTOMER_RFM_HOME_REVN_RATE_52W,
        CUSTOMER_RFM_HOME_DECOR_REVN_RATE_1W, CUSTOMER_RFM_HOME_DECOR_REVN_RATE_2W, CUSTOMER_RFM_HOME_DECOR_REVN_RATE_4W, CUSTOMER_RFM_HOME_DECOR_REVN_RATE_8W, CUSTOMER_RFM_HOME_DECOR_REVN_RATE_16W, CUSTOMER_RFM_HOME_DECOR_REVN_RATE_32W, CUSTOMER_RFM_HOME_DECOR_REVN_RATE_52W,
        CUSTOMER_RFM_JEWELRY_REVN_RATE_1W, CUSTOMER_RFM_JEWELRY_REVN_RATE_2W, CUSTOMER_RFM_JEWELRY_REVN_RATE_4W, CUSTOMER_RFM_JEWELRY_REVN_RATE_8W, CUSTOMER_RFM_JEWELRY_REVN_RATE_16W, CUSTOMER_RFM_JEWELRY_REVN_RATE_32W, CUSTOMER_RFM_JEWELRY_REVN_RATE_52W,
        CUSTOMER_RFM_KITCHEN_REVN_RATE_1W, CUSTOMER_RFM_KITCHEN_REVN_RATE_2W, CUSTOMER_RFM_KITCHEN_REVN_RATE_4W, CUSTOMER_RFM_KITCHEN_REVN_RATE_8W, CUSTOMER_RFM_KITCHEN_REVN_RATE_16W, CUSTOMER_RFM_KITCHEN_REVN_RATE_32W, CUSTOMER_RFM_KITCHEN_REVN_RATE_52W,
        CUSTOMER_RFM_NOVELTY_REVN_RATE_1W, CUSTOMER_RFM_NOVELTY_REVN_RATE_2W, CUSTOMER_RFM_NOVELTY_REVN_RATE_4W, CUSTOMER_RFM_NOVELTY_REVN_RATE_8W, CUSTOMER_RFM_NOVELTY_REVN_RATE_16W, CUSTOMER_RFM_NOVELTY_REVN_RATE_32W, CUSTOMER_RFM_NOVELTY_REVN_RATE_52W,
        CUSTOMER_RFM_SCHOOL_SUPPLIES_REVN_RATE_1W, CUSTOMER_RFM_SCHOOL_SUPPLIES_REVN_RATE_2W, CUSTOMER_RFM_SCHOOL_SUPPLIES_REVN_RATE_4W, CUSTOMER_RFM_SCHOOL_SUPPLIES_REVN_RATE_8W, CUSTOMER_RFM_SCHOOL_SUPPLIES_REVN_RATE_16W, CUSTOMER_RFM_SCHOOL_SUPPLIES_REVN_RATE_32W, CUSTOMER_RFM_SCHOOL_SUPPLIES_REVN_RATE_52W,
        CUSTOMER_RFM_TOYS_REVN_RATE_1W, CUSTOMER_RFM_TOYS_REVN_RATE_2W, CUSTOMER_RFM_TOYS_REVN_RATE_4W, CUSTOMER_RFM_TOYS_REVN_RATE_8W, CUSTOMER_RFM_TOYS_REVN_RATE_16W, CUSTOMER_RFM_TOYS_REVN_RATE_32W, CUSTOMER_RFM_TOYS_REVN_RATE_52W,
        CUSTOMER_RFM_TRAVEL_REVN_RATE_1W, CUSTOMER_RFM_TRAVEL_REVN_RATE_2W, CUSTOMER_RFM_TRAVEL_REVN_RATE_4W, CUSTOMER_RFM_TRAVEL_REVN_RATE_8W, CUSTOMER_RFM_TRAVEL_REVN_RATE_16W, CUSTOMER_RFM_TRAVEL_REVN_RATE_32W, CUSTOMER_RFM_TRAVEL_REVN_RATE_52W,
        CUSTOMER_RFM_TOTAL_REVN_RATE_1W, CUSTOMER_RFM_TOTAL_REVN_RATE_2W, CUSTOMER_RFM_TOTAL_REVN_RATE_4W, CUSTOMER_RFM_TOTAL_REVN_RATE_8W, CUSTOMER_RFM_TOTAL_REVN_RATE_16W, CUSTOMER_RFM_TOTAL_REVN_RATE_32W, CUSTOMER_RFM_TOTAL_REVN_RATE_52W

)
SELECT * FROM
(
        SELECT 
            CUSTOMERID, CAST(w.week_end_date AS TIMESTAMP) AS LAST_UPDATE_TS,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 7 THEN   CLOTHING_QTY END),0.0)      CUSTOMER_CLOTHING_RATE_1W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 14 THEN  CLOTHING_QTY END)/2.0,0.0)  CUSTOMER_CLOTHING_RATE_2W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 28 THEN  CLOTHING_QTY END)/4.0,0.0)  CUSTOMER_CLOTHING_RATE_4W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 56 THEN  CLOTHING_QTY END)/8.0,0.0)  CUSTOMER_CLOTHING_RATE_8W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 112 THEN CLOTHING_QTY END)/16.0,0.0) CUSTOMER_CLOTHING_RATE_16W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 224 THEN CLOTHING_QTY END)/32.0,0.0) CUSTOMER_CLOTHING_RATE_32W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 364 THEN CLOTHING_QTY END)/52.0,0.0) CUSTOMER_CLOTHING_RATE_52W,

            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 7 THEN   DELICATESSEN_QTY END),0.0)      CUSTOMER_DELICATESSEN_RATE_1W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 14 THEN  DELICATESSEN_QTY END)/2.0,0.0)  CUSTOMER_DELICATESSEN_RATE_2W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 28 THEN  DELICATESSEN_QTY END)/4.0,0.0)  CUSTOMER_DELICATESSEN_RATE_4W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 56 THEN  DELICATESSEN_QTY END)/8.0,0.0)  CUSTOMER_DELICATESSEN_RATE_8W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 112 THEN DELICATESSEN_QTY END)/16.0,0.0) CUSTOMER_DELICATESSEN_RATE_16W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 224 THEN DELICATESSEN_QTY END)/32.0,0.0) CUSTOMER_DELICATESSEN_RATE_32W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 364 THEN DELICATESSEN_QTY END)/52.0,0.0) CUSTOMER_DELICATESSEN_RATE_52W,

            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 7 THEN   GARDEN_QTY END),0.0)      CUSTOMER_GARDEN_RATE_1W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 14 THEN  GARDEN_QTY END)/2.0,0.0)  CUSTOMER_GARDEN_RATE_2W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 28 THEN  GARDEN_QTY END)/4.0,0.0)  CUSTOMER_GARDEN_RATE_4W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 56 THEN  GARDEN_QTY END)/8.0,0.0)  CUSTOMER_GARDEN_RATE_8W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 112 THEN GARDEN_QTY END)/16.0,0.0) CUSTOMER_GARDEN_RATE_16W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 224 THEN GARDEN_QTY END)/32.0,0.0) CUSTOMER_GARDEN_RATE_32W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 364 THEN GARDEN_QTY END)/52.0,0.0) CUSTOMER_GARDEN_RATE_52W,

            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 7 THEN   HOME_QTY END),0.0)      CUSTOMER_HOME_RATE_1W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 14 THEN  HOME_QTY END)/2.0,0.0)  CUSTOMER_HOME_RATE_2W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 28 THEN  HOME_QTY END)/4.0,0.0)  CUSTOMER_HOME_RATE_4W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 56 THEN  HOME_QTY END)/8.0,0.0)  CUSTOMER_HOME_RATE_8W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 112 THEN HOME_QTY END)/16.0,0.0) CUSTOMER_HOME_RATE_16W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 224 THEN HOME_QTY END)/32.0,0.0) CUSTOMER_HOME_RATE_32W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 364 THEN HOME_QTY END)/52.0,0.0) CUSTOMER_HOME_RATE_52W,

            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 7 THEN   HOME_DECOR_QTY END),0.0)      CUSTOMER_HOME_DECOR_RATE_1W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 14 THEN  HOME_DECOR_QTY END)/2.0,0.0)  CUSTOMER_HOME_DECOR_RATE_2W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 28 THEN  HOME_DECOR_QTY END)/4.0,0.0)  CUSTOMER_HOME_DECOR_RATE_4W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 56 THEN  HOME_DECOR_QTY END)/8.0,0.0)  CUSTOMER_HOME_DECOR_RATE_8W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 112 THEN HOME_DECOR_QTY END)/16.0,0.0) CUSTOMER_HOME_DECOR_RATE_16W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 224 THEN HOME_DECOR_QTY END)/32.0,0.0) CUSTOMER_HOME_DECOR_RATE_32W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 364 THEN HOME_DECOR_QTY END)/52.0,0.0) CUSTOMER_HOME_DECOR_RATE_52W,

            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 7 THEN   JEWELRY_QTY END),0.0)      CUSTOMER_JEWELRY_RATE_1W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 14 THEN  JEWELRY_QTY END)/2.0,0.0)  CUSTOMER_JEWELRY_RATE_2W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 28 THEN  JEWELRY_QTY END)/4.0,0.0)  CUSTOMER_JEWELRY_RATE_4W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 56 THEN  JEWELRY_QTY END)/8.0,0.0)  CUSTOMER_JEWELRY_RATE_8W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 112 THEN JEWELRY_QTY END)/16.0,0.0) CUSTOMER_JEWELRY_RATE_16W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 224 THEN JEWELRY_QTY END)/32.0,0.0) CUSTOMER_JEWELRY_RATE_32W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 364 THEN JEWELRY_QTY END)/52.0,0.0) CUSTOMER_JEWELRY_RATE_52W,

            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 7 THEN   KITCHEN_QTY END),0.0)      CUSTOMER_KITCHEN_RATE_1W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 14 THEN  KITCHEN_QTY END)/2.0,0.0)  CUSTOMER_KITCHEN_RATE_2W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 28 THEN  KITCHEN_QTY END)/4.0,0.0)  CUSTOMER_KITCHEN_RATE_4W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 56 THEN  KITCHEN_QTY END)/8.0,0.0)  CUSTOMER_KITCHEN_RATE_8W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 112 THEN KITCHEN_QTY END)/16.0,0.0) CUSTOMER_KITCHEN_RATE_16W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 224 THEN KITCHEN_QTY END)/32.0,0.0) CUSTOMER_KITCHEN_RATE_32W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 364 THEN KITCHEN_QTY END)/52.0,0.0) CUSTOMER_KITCHEN_RATE_52W,

            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 7 THEN   NOVELTY_QTY END),0.0)      CUSTOMER_NOVELTY_RATE_1W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 14 THEN  NOVELTY_QTY END)/2.0,0.0)  CUSTOMER_NOVELTY_RATE_2W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 28 THEN  NOVELTY_QTY END)/4.0,0.0)  CUSTOMER_NOVELTY_RATE_4W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 56 THEN  NOVELTY_QTY END)/8.0,0.0)  CUSTOMER_NOVELTY_RATE_8W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 112 THEN NOVELTY_QTY END)/16.0,0.0) CUSTOMER_NOVELTY_RATE_16W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 224 THEN NOVELTY_QTY END)/32.0,0.0) CUSTOMER_NOVELTY_RATE_32W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 364 THEN NOVELTY_QTY END)/52.0,0.0) CUSTOMER_NOVELTY_RATE_52W,

            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 7 THEN   SCHOOL_SUPPLIES_QTY END),0.0)      CUSTOMER_SCHOOL_SUPPLIES_RATE_1W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 14 THEN  SCHOOL_SUPPLIES_QTY END)/2.0,0.0)  CUSTOMER_SCHOOL_SUPPLIES_RATE_2W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 28 THEN  SCHOOL_SUPPLIES_QTY END)/4.0,0.0)  CUSTOMER_SCHOOL_SUPPLIES_RATE_4W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 56 THEN  SCHOOL_SUPPLIES_QTY END)/8.0,0.0)  CUSTOMER_SCHOOL_SUPPLIES_RATE_8W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 112 THEN SCHOOL_SUPPLIES_QTY END)/16.0,0.0) CUSTOMER_SCHOOL_SUPPLIES_RATE_16W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 224 THEN SCHOOL_SUPPLIES_QTY END)/32.0,0.0) CUSTOMER_SCHOOL_SUPPLIES_RATE_32W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 364 THEN SCHOOL_SUPPLIES_QTY END)/52.0,0.0) CUSTOMER_SCHOOL_SUPPLIES_RATE_52W,

            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 7 THEN   TOYS_QTY END),0.0)      CUSTOMER_TOYS_RATE_1W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 14 THEN  TOYS_QTY END)/2.0,0.0)  CUSTOMER_TOYS_RATE_2W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 28 THEN  TOYS_QTY END)/4.0,0.0)  CUSTOMER_TOYS_RATE_4W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 56 THEN  TOYS_QTY END)/8.0,0.0)  CUSTOMER_TOYS_RATE_8W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 112 THEN TOYS_QTY END)/16.0,0.0) CUSTOMER_TOYS_RATE_16W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 224 THEN TOYS_QTY END)/32.0,0.0) CUSTOMER_TOYS_RATE_32W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 364 THEN TOYS_QTY END)/52.0,0.0) CUSTOMER_TOYS_RATE_52W,

            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 7 THEN   TRAVEL_QTY END),0.0)      CUSTOMER_TRAVEL_RATE_1W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 14 THEN  TRAVEL_QTY END)/2.0,0.0)  CUSTOMER_TRAVEL_RATE_2W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 28 THEN  TRAVEL_QTY END)/4.0,0.0)  CUSTOMER_TRAVEL_RATE_4W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 56 THEN  TRAVEL_QTY END)/8.0,0.0)  CUSTOMER_TRAVEL_RATE_8W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 112 THEN TRAVEL_QTY END)/16.0,0.0) CUSTOMER_TRAVEL_RATE_16W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 224 THEN TRAVEL_QTY END)/32.0,0.0) CUSTOMER_TRAVEL_RATE_32W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 364 THEN TRAVEL_QTY END)/52.0,0.0) CUSTOMER_TRAVEL_RATE_52W,

            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 7 THEN   TOTAL_QTY END),0.0)      CUSTOMER_TOTAL_RATE_1W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 14 THEN  TOTAL_QTY END)/2.0,0.0)  CUSTOMER_TOTAL_RATE_2W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 28 THEN  TOTAL_QTY END)/4.0,0.0)  CUSTOMER_TOTAL_RATE_4W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 56 THEN  TOTAL_QTY END)/8.0,0.0)  CUSTOMER_TOTAL_RATE_8W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 112 THEN TOTAL_QTY END)/16.0,0.0) CUSTOMER_TOTAL_RATE_16W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 224 THEN TOTAL_QTY END)/32.0,0.0) CUSTOMER_TOTAL_RATE_32W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 364 THEN TOTAL_QTY END)/52.0,0.0) CUSTOMER_TOTAL_RATE_52W,

            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 7 THEN   CLOTHING_REVENUE END),0.0)      CUSTOMER_CLOTHING_REVN_RATE_1W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 14 THEN  CLOTHING_REVENUE END)/2.0,0.0)  CUSTOMER_CLOTHING_REVN_RATE_2W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 28 THEN  CLOTHING_REVENUE END)/4.0,0.0)  CUSTOMER_CLOTHING_REVN_RATE_4W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 56 THEN  CLOTHING_REVENUE END)/8.0,0.0)  CUSTOMER_CLOTHING_REVN_RATE_8W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 112 THEN CLOTHING_REVENUE END)/16.0,0.0) CUSTOMER_CLOTHING_REVN_RATE_16W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 224 THEN CLOTHING_REVENUE END)/32.0,0.0) CUSTOMER_CLOTHING_REVN_RATE_32W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 364 THEN CLOTHING_REVENUE END)/52.0,0.0) CUSTOMER_CLOTHING_REVN_RATE_52W,

            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 7 THEN   DELICATESSEN_REVENUE END),0.0)      CUSTOMER_DELICATESSEN_REVN_RATE_1W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 14 THEN  DELICATESSEN_REVENUE END)/2.0,0.0)  CUSTOMER_DELICATESSEN_REVN_RATE_2W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 28 THEN  DELICATESSEN_REVENUE END)/4.0,0.0)  CUSTOMER_DELICATESSEN_REVN_RATE_4W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 56 THEN  DELICATESSEN_REVENUE END)/8.0,0.0)  CUSTOMER_DELICATESSEN_REVN_RATE_8W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 112 THEN DELICATESSEN_REVENUE END)/16.0,0.0) CUSTOMER_DELICATESSEN_REVN_RATE_16W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 224 THEN DELICATESSEN_REVENUE END)/32.0,0.0) CUSTOMER_DELICATESSEN_REVN_RATE_32W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 364 THEN DELICATESSEN_REVENUE END)/52.0,0.0) CUSTOMER_DELICATESSEN_REVN_RATE_52W,

            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 7 THEN   GARDEN_REVENUE END),0.0)      CUSTOMER_GARDEN_REVN_RATE_1W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 14 THEN  GARDEN_REVENUE END)/2.0,0.0)  CUSTOMER_GARDEN_REVN_RATE_2W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 28 THEN  GARDEN_REVENUE END)/4.0,0.0)  CUSTOMER_GARDEN_REVN_RATE_4W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 56 THEN  GARDEN_REVENUE END)/8.0,0.0)  CUSTOMER_GARDEN_REVN_RATE_8W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 112 THEN GARDEN_REVENUE END)/16.0,0.0) CUSTOMER_GARDEN_REVN_RATE_16W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 224 THEN GARDEN_REVENUE END)/32.0,0.0) CUSTOMER_GARDEN_REVN_RATE_32W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 364 THEN GARDEN_REVENUE END)/52.0,0.0) CUSTOMER_GARDEN_REVN_RATE_52W,

            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 7 THEN   HOME_REVENUE END),0.0)      CUSTOMER_HOME_REVN_RATE_1W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 14 THEN  HOME_REVENUE END)/2.0,0.0)  CUSTOMER_HOME_REVN_RATE_2W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 28 THEN  HOME_REVENUE END)/4.0,0.0)  CUSTOMER_HOME_REVN_RATE_4W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 56 THEN  HOME_REVENUE END)/8.0,0.0)  CUSTOMER_HOME_REVN_RATE_8W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 112 THEN HOME_REVENUE END)/16.0,0.0) CUSTOMER_HOME_REVN_RATE_16W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 224 THEN HOME_REVENUE END)/32.0,0.0) CUSTOMER_HOME_REVN_RATE_32W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 364 THEN HOME_REVENUE END)/52.0,0.0) CUSTOMER_HOME_REVN_RATE_52W,

            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 7 THEN   HOME_DECOR_REVENUE END),0.0)      CUSTOMER_HOME_DECOR_REVN_RATE_1W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 14 THEN  HOME_DECOR_REVENUE END)/2.0,0.0)  CUSTOMER_HOME_DECOR_REVN_RATE_2W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 28 THEN  HOME_DECOR_REVENUE END)/4.0,0.0)  CUSTOMER_HOME_DECOR_REVN_RATE_4W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 56 THEN  HOME_DECOR_REVENUE END)/8.0,0.0)  CUSTOMER_HOME_DECOR_REVN_RATE_8W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 112 THEN HOME_DECOR_REVENUE END)/16.0,0.0) CUSTOMER_HOME_DECOR_REVN_RATE_16W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 224 THEN HOME_DECOR_REVENUE END)/32.0,0.0) CUSTOMER_HOME_DECOR_REVN_RATE_32W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 364 THEN HOME_DECOR_REVENUE END)/52.0,0.0) CUSTOMER_HOME_DECOR_REVN_RATE_52W,

            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 7 THEN   JEWELRY_REVENUE END),0.0)      CUSTOMER_JEWELRY_REVN_RATE_1W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 14 THEN  JEWELRY_REVENUE END)/2.0,0.0)  CUSTOMER_JEWELRY_REVN_RATE_2W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 28 THEN  JEWELRY_REVENUE END)/4.0,0.0)  CUSTOMER_JEWELRY_REVN_RATE_4W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 56 THEN  JEWELRY_REVENUE END)/8.0,0.0)  CUSTOMER_JEWELRY_REVN_RATE_8W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 112 THEN JEWELRY_REVENUE END)/16.0,0.0) CUSTOMER_JEWELRY_REVN_RATE_16W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 224 THEN JEWELRY_REVENUE END)/32.0,0.0) CUSTOMER_JEWELRY_REVN_RATE_32W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 364 THEN JEWELRY_REVENUE END)/52.0,0.0) CUSTOMER_JEWELRY_REVN_RATE_52W,

            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 7 THEN   KITCHEN_REVENUE END),0.0)      CUSTOMER_KITCHEN_REVN_RATE_1W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 14 THEN  KITCHEN_REVENUE END)/2.0,0.0)  CUSTOMER_KITCHEN_REVN_RATE_2W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 28 THEN  KITCHEN_REVENUE END)/4.0,0.0)  CUSTOMER_KITCHEN_REVN_RATE_4W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 56 THEN  KITCHEN_REVENUE END)/8.0,0.0)  CUSTOMER_KITCHEN_REVN_RATE_8W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 112 THEN KITCHEN_REVENUE END)/16.0,0.0) CUSTOMER_KITCHEN_REVN_RATE_16W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 224 THEN KITCHEN_REVENUE END)/32.0,0.0) CUSTOMER_KITCHEN_REVN_RATE_32W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 364 THEN KITCHEN_REVENUE END)/52.0,0.0) CUSTOMER_KITCHEN_REVN_RATE_52W,

            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 7 THEN   NOVELTY_REVENUE END),0.0)      CUSTOMER_NOVELTY_REVN_RATE_1W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 14 THEN  NOVELTY_REVENUE END)/2.0,0.0)  CUSTOMER_NOVELTY_REVN_RATE_2W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 28 THEN  NOVELTY_REVENUE END)/4.0,0.0)  CUSTOMER_NOVELTY_REVN_RATE_4W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 56 THEN  NOVELTY_REVENUE END)/8.0,0.0)  CUSTOMER_NOVELTY_REVN_RATE_8W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 112 THEN NOVELTY_REVENUE END)/16.0,0.0) CUSTOMER_NOVELTY_REVN_RATE_16W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 224 THEN NOVELTY_REVENUE END)/32.0,0.0) CUSTOMER_NOVELTY_REVN_RATE_32W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 364 THEN NOVELTY_REVENUE END)/52.0,0.0) CUSTOMER_NOVELTY_REVN_RATE_52W,

            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 7 THEN   SCHOOL_SUPPLIES_REVENUE END),0.0)      CUSTOMER_SCHOOL_SUPPLIES_REVN_RATE_1W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 14 THEN  SCHOOL_SUPPLIES_REVENUE END)/2.0,0.0)  CUSTOMER_SCHOOL_SUPPLIES_REVN_RATE_2W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 28 THEN  SCHOOL_SUPPLIES_REVENUE END)/4.0,0.0)  CUSTOMER_SCHOOL_SUPPLIES_REVN_RATE_4W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 56 THEN  SCHOOL_SUPPLIES_REVENUE END)/8.0,0.0)  CUSTOMER_SCHOOL_SUPPLIES_REVN_RATE_8W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 112 THEN SCHOOL_SUPPLIES_REVENUE END)/16.0,0.0) CUSTOMER_SCHOOL_SUPPLIES_REVN_RATE_16W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 224 THEN SCHOOL_SUPPLIES_REVENUE END)/32.0,0.0) CUSTOMER_SCHOOL_SUPPLIES_REVN_RATE_32W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 364 THEN SCHOOL_SUPPLIES_REVENUE END)/52.0,0.0) CUSTOMER_SCHOOL_SUPPLIES_REVN_RATE_52W,

            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 7 THEN   TOYS_REVENUE END),0.0)      CUSTOMER_TOYS_REVN_RATE_1W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 14 THEN  TOYS_REVENUE END)/2.0,0.0)  CUSTOMER_TOYS_REVN_RATE_2W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 28 THEN  TOYS_REVENUE END)/4.0,0.0)  CUSTOMER_TOYS_REVN_RATE_4W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 56 THEN  TOYS_REVENUE END)/8.0,0.0)  CUSTOMER_TOYS_REVN_RATE_8W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 112 THEN TOYS_REVENUE END)/16.0,0.0) CUSTOMER_TOYS_REVN_RATE_16W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 224 THEN TOYS_REVENUE END)/32.0,0.0) CUSTOMER_TOYS_REVN_RATE_32W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 364 THEN TOYS_REVENUE END)/52.0,0.0) CUSTOMER_TOYS_REVN_RATE_52W,

            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 7 THEN   TRAVEL_REVENUE END),0.0)      CUSTOMER_TRAVEL_REVN_RATE_1W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 14 THEN  TRAVEL_REVENUE END)/2.0,0.0)  CUSTOMER_TRAVEL_REVN_RATE_2W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 28 THEN  TRAVEL_REVENUE END)/4.0,0.0)  CUSTOMER_TRAVEL_REVN_RATE_4W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 56 THEN  TRAVEL_REVENUE END)/8.0,0.0)  CUSTOMER_TRAVEL_REVN_RATE_8W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 112 THEN TRAVEL_REVENUE END)/16.0,0.0) CUSTOMER_TRAVEL_REVN_RATE_16W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 224 THEN TRAVEL_REVENUE END)/32.0,0.0) CUSTOMER_TRAVEL_REVN_RATE_32W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 364 THEN TRAVEL_REVENUE END)/52.0,0.0) CUSTOMER_TRAVEL_REVN_RATE_52W,

            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 7 THEN   TOTAL_REVENUE END),0.0)      CUSTOMER_TOTAL_REVN_RATE_1W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 14 THEN  TOTAL_REVENUE END)/2.0,0.0)  CUSTOMER_TOTAL_REVN_RATE_2W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 28 THEN  TOTAL_REVENUE END)/4.0,0.0)  CUSTOMER_TOTAL_REVN_RATE_4W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 56 THEN  TOTAL_REVENUE END)/8.0,0.0)  CUSTOMER_TOTAL_REVN_RATE_8W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 112 THEN TOTAL_REVENUE END)/16.0,0.0) CUSTOMER_TOTAL_REVN_RATE_16W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 224 THEN TOTAL_REVENUE END)/32.0,0.0) CUSTOMER_TOTAL_REVN_RATE_32W,
            COALESCE(SUM(CASE WHEN w.week_end_date - INVOICEDATE BETWEEN 1 AND 364 THEN TOTAL_REVENUE END)/52.0,0.0) CUSTOMER_TOTAL_REVN_RATE_52W

        FROM RETAIL_RFM.CUSTOMER_CATEGORY_ACTIVITY cca --splice-properties splits=52
        INNER JOIN (SELECT max(week_end_date) as week_end_date FROM RETAIL_RFM.WEEKS WHERE week_end_date <= CURRENT_DATE) w 
           ON cca.INVOICEDATE BETWEEN w.week_end_date-364 AND w.week_end_date - 1   -- a whole year ending on the week_end_date for each week period
        WHERE cca.CUSTOMERID IS NOT NULL
        GROUP BY 1,2
    
)y
WHERE NOT EXISTS ( SELECT 1 FROM RETAIL_FS.CUSTOMER_RFM_BY_CATEGORY WHERE CUSTOMERID=y.CUSTOMERID);


--gather DB statistics
ANALYZE SCHEMA retail_fs;

