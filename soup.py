from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
import pandas as pd
import time
import sqlite3
from datetime import datetime

def scrape_baseball_data(start_year=1993, end_year=2024):
    # Set up the Chrome driver
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
    
    # Connect to SQLite database
    conn = sqlite3.connect('rockies_pitching_splits.db')
    
    # Create master table for team splits
    conn.execute('''
        CREATE TABLE IF NOT EXISTS pitching_splits (
            split_type TEXT,
            game_count INTEGER,
            innings_pitched REAL,
            runs_allowed INTEGER,
            earned_runs INTEGER,
            era REAL,
            hits INTEGER,
            home_runs INTEGER,
            walks INTEGER,
            strikeouts INTEGER,
            batting_avg REAL,
            onbase_pct REAL,
            slugging_pct REAL,
            ops REAL,
            year INTEGER,
            PRIMARY KEY (split_type, year)
        )
    ''')
    
    try:
        for year in range(start_year, end_year + 1):
            url = f"https://www.baseball-reference.com/teams/split.cgi?t=p&team=COL&year={year}"
            driver.get(url)
            
            # Wait for content to load
            time.sleep(3)
            
            # Find all tables
            table_elements = driver.find_elements(By.CLASS_NAME, "statistics")
            
            for table_element in table_elements:
                table_html = table_element.get_attribute("outerHTML")
                try:
                    df = pd.read_html(table_html)[0]
                    
                    # Clean the dataframe
                    # Remove multi-level column headers if they exist
                    if isinstance(df.columns, pd.MultiIndex):
                        df.columns = df.columns.get_level_values(-1)
                    
                    # Remove rows that are section headers or totals
                    df = df[df['Split'].str.contains('Home|Away|Day|Night|Grass|Turf|Indoor|Outdoor', na=False)]
                    
                    # Clean and rename columns
                    df = df.rename(columns={
                        'Split': 'split_type',
                        'G': 'game_count',
                        'IP': 'innings_pitched',
                        'R': 'runs_allowed',
                        'ER': 'earned_runs',
                        'ERA': 'era',
                        'H': 'hits',
                        'HR': 'home_runs',
                        'BB': 'walks',
                        'SO': 'strikeouts',
                        'BA': 'batting_avg',
                        'OBP': 'onbase_pct',
                        'SLG': 'slugging_pct',
                        'OPS': 'ops'
                    })
                    
                    # Add year column
                    df['year'] = year
                    
                    # Select and reorder columns
                    columns = ['split_type', 'game_count', 'innings_pitched', 'runs_allowed',
                             'earned_runs', 'era', 'hits', 'home_runs', 'walks', 'strikeouts',
                             'batting_avg', 'onbase_pct', 'slugging_pct', 'ops', 'year']
                    
                    df = df[columns]
                    
                    # Convert numeric columns
                    numeric_columns = ['game_count', 'innings_pitched', 'runs_allowed', 'earned_runs',
                                     'era', 'hits', 'home_runs', 'walks', 'strikeouts', 'batting_avg',
                                     'onbase_pct', 'slugging_pct', 'ops']
                    
                    for col in numeric_columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                    
                    # Insert data into database
                    df.to_sql('pitching_splits', conn, if_exists='append', index=False)
                    
                    print(f"Successfully processed data for year {year}")
                    
                except ValueError as e:
                    print(f"Error processing table for year {year}: {e}")
                except Exception as e:
                    print(f"Unexpected error for year {year}: {e}")
            
            # Add delay between requests
            time.sleep(2)
            
    except Exception as e:
        print(f"Error during scraping: {e}")
    finally:
        # Create indexes for better query performance
        conn.execute('CREATE INDEX IF NOT EXISTS idx_year ON pitching_splits(year)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_split ON pitching_splits(split_type)')
        
        # Close connections
        conn.close()
        driver.quit()

if __name__ == "__main__":
    scrape_baseball_data()