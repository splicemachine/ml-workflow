
time_conversions = {
    "s-m":60,
    "s-h":60*60,
    "s-d":60*60*24,
    "s-w":60*60*24*7,
    "s-mn":60*60*24*30, # 30 days in a month
    "s-q":60*60*24*7*13,  # 13 week in a quarter
    "s-y":60*60*24*365, # 365 days in a year
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
