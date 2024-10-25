from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
import pandas as pd
import time
import sqlite3

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))

url = "https://baseballsavant.mlb.com/leaderboard/pitcher-arm-angles?season=2024&team=115&pitchHand=&min=q&sort=ascending"
driver.get(url)

time.sleep(5)

table_elements = driver.find_elements(By.TAG_NAME, "svg")

tables = []
for table_element in table_elements:
    table_html = table_element.get_attribute("class")
    try:
        table_df = pd.read_html(table_html)[0]
        tables.append(table_df)
    except ValueError:
        print(f"Skipping table: {table_html}")

conn = sqlite3.connect('baseball_angle.db')

for i, table in enumerate(tables):
    table_name = f"table_{i+1}"
    
    table.columns = table.columns.astype(str).str.replace('/', '_')
    
    table.to_sql(table_name, conn, if_exists='replace', index=False)

conn.close()

driver.quit()