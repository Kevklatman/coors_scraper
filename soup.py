from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
import pandas as pd
import time
import sqlite3

# Set up the Chrome driver
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))

# Navigate to the webpage
url = "https://www.baseball-reference.com/teams/split.cgi?t=p&team=COL&year=2024"
driver.get(url)

# Wait for the page to load (adjust the sleep time if needed)
time.sleep(5)

# Find all the table elements on the page
table_elements = driver.find_elements(By.TAG_NAME, "table")

# Extract the table data and store it in a list of DataFrames
tables = []
for table_element in table_elements:
    table_html = table_element.get_attribute("outerHTML")
    try:
        table_df = pd.read_html(table_html)[0]
        tables.append(table_df)
    except ValueError:
        print(f"Skipping table: {table_html}")

# ###Connect to the SQLite database (it will create a new file if it doesn't exist)
conn = sqlite3.connect('baseball_data.db')

# Create tables in the database to store the data
for i, table in enumerate(tables):
    table_name = f"table_{i+1}"
    
    # Replace '/' with '_' in column names to avoid SQL syntax errors
    table.columns = table.columns.str.replace('/', '_')
    
    # Insert the table data into the database
    table.to_sql(table_name, conn, if_exists='replace', index=False)

# Close the database connection
conn.close()

# Close the browser
driver.quit()