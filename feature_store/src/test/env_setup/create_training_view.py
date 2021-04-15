from splicemachine.features import FeatureStore
fs = FeatureStore()
fs.set_feature_store_url('http://localhost:8000')
fs.login_fs('local','testing')


try:
    fs.remove_training_view('customer_lifetime_value')
except:
    print("Training view customer_lifetime_value doesnt exist")

sql = """
SELECT ltv.CUSTOMERID, 
       ((w.WEEK_END_DATE - ltv.CUSTOMER_START_DATE)/ 7) CUSTOMERWEEK,
       CAST(w.WEEK_END_DATE as TIMESTAMP) CUSTOMER_TS,  
       ltv.CUSTOMER_LIFETIME_VALUE as CUSTOMER_LTV
FROM retail_rfm.weeks w --splice-properties useSpark=True
INNER JOIN 
    retail_fs.customer_lifetime ltv 
    ON w.WEEK_END_DATE > ltv.CUSTOMER_START_DATE AND w.WEEK_END_DATE <= ltv.CUSTOMER_START_DATE + 28 --only first 4 weeks
"""
pks = ['CUSTOMERID','CUSTOMERWEEK'] # Each unique training row is identified by the customer and their week of spending activity
join_keys = ['CUSTOMERID'] # This is the primary key of the Feature Sets that we want to join to
fs.create_training_view(
    'customer_lifetime_value',
    sql=sql,
    primary_keys=pks,
    join_keys=join_keys,
    ts_col = 'CUSTOMER_TS', # How we join each unique row with our eventual Features
    label_col='CUSTOMER_LTV', # The thing we want to predict
    desc = 'The current (as of queried) lifetime value of each customer per week of being a customer'
)


try:
    fs.remove_training_view('Customer_Product_Affinity')
except:
    print("Training view Customer_Product_Affinity does not exist")


sql = """
SELECT txn.CUSTOMERID, 
       ol1.STOCKCODE,
       CAST(txn.INVOICEDATE as TIMESTAMP) CUSTOMER_TS,  
       ol2.STOCKCODE STOCKCODE1 
FROM retail.ORDER_TXN txn
INNER JOIN 
    retail.ORDER_LINE ol1 ON txn.INVOICE=ol1.INVOICE
INNER JOIN
    retail.ORDER_LINE ol2 ON txn.INVOICE=ol2.INVOICE AND ol1.STOCKCODE <> ol2.STOCKCODE
"""
pks = ['CUSTOMERID','STOCKCODE'] # Each unique training row is identified by the customer and their week of spending activity
join_keys = ['CUSTOMERID'] # This is the primary key of the Feature Sets that we want to join to
fs.create_training_view(
    'Customer_Product_Affinity',
    sql=sql,
    primary_keys=pks,
    join_keys=join_keys,
    ts_col = 'CUSTOMER_TS', # How we join each unique row with our eventual Features
    desc = 'The current (as of queried) lifetime value of each customer per week of being a customer'
)
