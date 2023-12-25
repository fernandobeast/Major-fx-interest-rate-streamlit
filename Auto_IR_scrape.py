from ir_util import create_chrome_webdriver,postgre_sql_connection,mapping_all_curr
import pandas as pd
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from time import sleep as ts

from sqlalchemy.types import DATE,CHAR,NUMERIC

# interest-rate-decision- mapping, add the number at the end of - "AED":"1987"
invcom_mapping = {"USD":"168","CHF":"169","GBP":"170",
                  "AUD":"171","CAD":"166","NZD":"167",
                  "JPY":"165","EUR":"164","HKD":"2104"}

# get the latest up to day all currency interest rate from database
interstrate_db = postgre_sql_connection('streamlit')
latest_rate_query = '''WITH RankedRates AS (
    SELECT release_date,currency,actual_rate,
        ROW_NUMBER() OVER (PARTITION BY currency ORDER BY release_date DESC) AS row_num
    FROM interest_rate)
SELECT release_date,currency , actual_rate
FROM RankedRates WHERE row_num = 1;'''

df_original = interstrate_db.postgre_download_data(query=latest_rate_query)
df_original['release_date'] = pd.to_datetime(df_original['release_date'])

# webscrape the latest currency row
print('start webscraping.........................')
scraped_df = mapping_all_curr(invcom_mapping)

# remove duplicates between new data and old ones
test = pd.merge(scraped_df,df_original,on=['currency','release_date','actual_rate'],how='outer',indicator=True)
diff_df = test.query("_merge == 'left_only'").iloc[:,:-1]


if diff_df.empty:
    print('no recent interest rate release has been found')
else:
    interest_rate_schema = {"release_date": DATE,
                    "currency": CHAR(3),
                    "actual_rate":NUMERIC(4,2)}
    interstrate_db.postgre_upload_data(df=diff_df,schema=interest_rate_schema,table='interest_rate')
