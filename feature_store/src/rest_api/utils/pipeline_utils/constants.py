time_conversions = {
    "s-m":60,
    "s-h":3600,
    "s-d":86400,
    "s-w":604800,
    "s-mn":2592000, # 30 days in month
    "s-q":7862400,  # 13 week quarter
    "s-y":31536000, # 365days
    "m-s":1.0/60.0,
    "m-h":60,
    "m-d":1440,
    "m-w":10080,
    "m-mn":43200, # 30 days in month
    "m-q":131040, # 13 week quarter
    "m-y":525600, # 365 days
    "h-s":1.0/3600.0,
    "h-m":1.0/60.0,
    "h-d":24,
    "h-w":168,
    "h-mn":720, # 30 days in month
    "h-q":2184, # 13 week quarter
    "h-y":8760, # 365 days
    "d-s":0.00001157407407,
    "d-m":0.0006944444444,
    "d-h":0.04166666667,
    "d-w":7,
    "d-mn":30, # 30 days in month
    "d-q":91, # 13 week quarter
    "d-y":365, # 365 days
    "w-s":0.0000016534391,
    "w-m":0.00009920634921,
    "w-h":0.005952380952,
    "w-d":0.1428571429,
    "w-mn":4.2857, # 30 days in month
    "w-q":13, # 13 week quarter
    "w-y":52, # 365 days
    "mn-s":0.000000385802469,
    "mn-m":0.00002314814815,
    "mn-h":0.001388888889,
    "mn-d":0.03333333333,
    "mn-w":0.2333341111, # 30 days in month
    "mn-q":3, # 13 week quarter
    "mn-y":12, # 365 days
    "y-s":0.000000031709792,
    "y-m":0.000001902587519,
    "y-h":0.0001141552511,
    "y-d":0.002739726027,
    "y-w":0.01923076923, # 30 days in month
    "y-mn":0.08333333333, # 13 week quarter
    "y-q":0.25, # 365 days
}

# SQL needs the window values as their numeric representations. Does not support the Enum
tsi_window_values = {
"s":1,
"m":2,
"h":3,
"d":4,
"w":5,
"mn":6,
"q":7,
"y":8
}

tsi_windows = {  # Valid splice machine timestamp windows
    "s":"SQL_TSI_SECOND",
    "m":"SQL_TSI_MINUTE",
    "h":"SQL_TSI_HOUR",
    "d":"SQL_TSI_DAY",
    "w":"SQL_TSI_WEEK",
    "mn":"SQL_TSI_MONTH",
    "q":"SQL_TSI_QUARTER",
    "y":"SQL_TSI_YEAR"
}

window_to_interval_keyword = {
"s":"second",
"m":"minute",
"h":"hour",
"d":"day",
"w":"day", # week isn't supported in Splice SQL so we will convert to "7 day"
"mn":"month",
"q":"month", # quarter isn't supported in Splice SQL  so we will user "3 month"
"y":"year"
}
