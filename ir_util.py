import pandas as pd
from sqlalchemy import create_engine,MetaData
from IPython.display import display

class postgre_sql_connection:
    def __init__(self,sql_dbname):
        # SQL configuration
        self.sql_username = 'your_username'
        self.sql_password = 'your_password'
        self.sql_ipaddress = 'your_ip'
        self.sql_port = 'your_port_in_int'
        self.sql_dbname = sql_dbname
        self.postgres_str = f'postgresql://{self.sql_username}:{self.sql_password}@{self.sql_ipaddress}:{self.sql_port}/{self.sql_dbname}'
        self.connection_port = create_engine(self.postgres_str)

    def postgre_download_data(self,query: str) -> pd.DataFrame:
        data = pd.read_sql(query,self.connection_port)
        return data
    
    def postgre_upload_data(self, schema:dict, df:pd.DataFrame=None, table:str =None):
        if df is None and table is None:
            raise ValueError('df or table name is missing')
        else:
# fetch all the table names from database
            metadata = MetaData()
            metadata.bind = self.connection_port
            metadata.reflect(metadata.bind)
            table_names = list(metadata.tables.keys())
            print(f'check the schema: \n\n{schema}\n\ntable name: {table}')
            

            if table.lower() in table_names:
                display(df.head())
                if df.to_sql(table.lower(),con=self.connection_port, index=False,
                                    if_exists='append', dtype=schema, method='multi',chunksize=20000):
                    print("data has been uploaded successfully")
                    self.connection_port.dispose()
                else:
                    print(f"upload to db:{self.sql_dbname} table:{table} failed")
            else:
                raise ValueError("table name is incorrect")
    
# selenium webscraping
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from faker import Faker

def create_chrome_webdriver():
    chrome_options = Options()
    # run it without UI
    chrome_options.add_argument('--headless=new')
    #chrome_options.add_argument('--disable-gpu')
    # Invalid or expired SLL will bloack access or warning msg
    chrome_options.add_argument("--ignore-certificate-errors")
    # extra popup from the website
    #chrome_options.add_argument("--disable-popup-blocking")
    chrome_options.add_argument("--disable-infobars")
    chrome_options.add_argument("--disable-notifications")

    # disguise as a normal user
    fake = Faker()    
    chrome_options.add_argument(fake.user_agent())
    chrome_options.add_argument('--disable-blink-features=AutomationControlled')

    #chrome_options.add_argument("--disable-web-security")
    # path for VScode
    service = Service(r'/home/fernando/Documents/Python/Web Scrape Related/chromedriver')
    return webdriver.Chrome(service=service,options=chrome_options)

# driver = create_chrome_webdriver()
# driver.get(url)

import re

def mapping_all_curr(mapping_dict: dict) -> pd.DataFrame:
    ''' web scrape all currencies mapped on investing.com and return
    latest rates
    '''

    def recent_rate_filtering(selenium_result: str,currency: str):
        '''Only applicable for up to date interest rate scrape, the first
    row of the column
        '''
        # regex out all the strings contain (something)
        lines = re.sub(r'\([^)]*\)', '', selenium_result).split('\n')
    #remove column names and split each lines to lists
        remove_columns = [line.split() for line in lines[1:]]
    # if len is 6 means not actual rate released yet
        if len(remove_columns[0]) == 7:
    # 2nd value joins the data from first 3 eles, and select 4th pos of the list        
            prelist = [' '.join(remove_columns[0][:3]),currency,remove_columns[0][4]]
            return prelist
        
    final_list = []
    # mapping each currencies
    for c,idx in mapping_dict.items():
        url = f'https://www.investing.com/economic-calendar/interest-rate-decision-{idx}'
        driver = create_chrome_webdriver()
        driver.get(url)
        try:
            result = WebDriverWait(driver, 20).until(EC.visibility_of_element_located((By.CSS_SELECTOR, f"#eventHistoryTable{idx}"))).text
        except Exception as e:
            print(f'\nscrapping error with {c},{idx}\n')
            continue
# regardless of success or failure, it quits
        finally:
            driver.quit()

        scrape_list = recent_rate_filtering(result,currency=c)
        
        if scrape_list is not None:
            final_list.extend([scrape_list])

    df_update = pd.DataFrame(columns=['release_date','currency','actual_rate'],data=final_list)
    df_update['release_date'] = pd.to_datetime(df_update['release_date'],infer_datetime_format=True)
    df_update['actual_rate'] = df_update['actual_rate'].str.replace('%','').astype('float')

    return df_update
