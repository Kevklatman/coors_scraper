from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import os
import sqlite3
from datetime import datetime
import time

class BaseballSplitsDB:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.setup_database()
        print(f"Database initialized at {db_path}")

    def setup_database(self):
        # Drop existing tables to ensure clean schema
        self.conn.execute("DROP TABLE IF EXISTS splits_data")
        self.conn.execute("DROP TABLE IF EXISTS game_level_splits")
        
        # Create table for regular splits with exact column names from the data
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS splits_data (
                category TEXT,
                Split TEXT,
                G TEXT,
                PA TEXT,
                AB TEXT,
                R TEXT,
                H TEXT,
                [2B] TEXT,
                [3B] TEXT,
                HR TEXT,
                SB TEXT,
                CS TEXT,
                BB TEXT,
                SO TEXT,
                [SO/W] TEXT,
                BA TEXT,
                OBP TEXT,
                SLG TEXT,
                OPS TEXT,
                TB TEXT,
                GDP TEXT,
                HBP TEXT,
                SH TEXT,
                SF TEXT,
                IBB TEXT,
                ROE TEXT,
                BAbip TEXT,
                [tOPS+] TEXT,
                [sOPS+] TEXT,
                capture_date TIMESTAMP
            )
        """)
        
        # Create table for game-level splits with exact column names
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS game_level_splits (
                category TEXT,
                Split TEXT,
                W TEXT,
                L TEXT,
                [W-L%] TEXT,
                ERA TEXT,
                G TEXT,
                GS TEXT,
                GF TEXT,
                CG TEXT,
                SHO TEXT,
                SV TEXT,
                IP TEXT,
                H TEXT,
                R TEXT,
                ER TEXT,
                HR TEXT,
                BB TEXT,
                IBB TEXT,
                SO TEXT,
                HBP TEXT,
                BK TEXT,
                WP TEXT,
                BF TEXT,
                WHIP TEXT,
                SO9 TEXT,
                [SO/W] TEXT,
                capture_date TIMESTAMP
            )
        """)
        
        self.conn.commit()
        print("Database tables created with correct schema")

    def store_splits_data(self, category: str, df: pd.DataFrame, is_game_level: bool = False):
        print(f"\nStoring data for category: {category}")
        print(f"DataFrame shape: {df.shape}")
        
        capture_date = datetime.now()
        table_name = "game_level_splits" if is_game_level else "splits_data"
        
        # Add category and capture date
        df['category'] = category
        df['capture_date'] = capture_date
        
        # Convert all numeric columns to strings to avoid type issues
        for col in df.columns:
            if col not in ['category', 'capture_date']:
                df[col] = df[col].astype(str)
        
        try:
            # Use pandas to_sql with correct parameters
            df.to_sql(table_name, self.conn, if_exists='append', index=False,
                     dtype={col: 'TEXT' for col in df.columns if col not in ['category', 'capture_date']})
            self.conn.commit()
            print(f"Successfully stored {len(df)} records in {table_name}")
            
            # Verify the data was stored
            count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name} WHERE category = ?", 
                                    (category,)).fetchone()[0]
            print(f"Verified {count} records in database for category: {category}")
            
        except Exception as e:
            print(f"Error storing data: {e}")
            print("DataFrame columns:", df.columns.tolist())

class BaseballReferenceScraper:
    def __init__(self, base_path: str):
        self.base_path = base_path
        db_path = os.path.join(base_path, "baseball_splits_2024.db")
        print(f"Initializing scraper with database at: {db_path}")
        self.db = BaseballSplitsDB(db_path)
        self.driver = None

    def setup_driver(self):
        try:
            chrome_options = Options()
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--window-size=1920,1080")
            
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            print("Chrome WebDriver initialized successfully")
            
        except Exception as e:
            print(f"Error setting up WebDriver: {e}")
            raise

    def cleanup(self):
        if self.driver:
            try:
                self.driver.quit()
                print("WebDriver cleaned up successfully")
            except Exception as e:
                print(f"Error during cleanup: {e}")

    def get_table_html(self, table) -> str:
        return self.driver.execute_script("return arguments[0].outerHTML;", table)

    def extract_table_data(self, table_html: str, category: str) -> pd.DataFrame:
        try:
            # Use pandas to read the HTML table
            dfs = pd.read_html(table_html)
            if dfs:
                df = dfs[0]
                print(f"Extracted table with shape: {df.shape}")
                print("Columns:", df.columns.tolist())
                return df
        except Exception as e:
            print(f"Error extracting table data: {e}")
        return pd.DataFrame()

    def get_category_name(self, table) -> str:
        try:
            return self.driver.execute_script("""
                let element = arguments[0];
                while (element.previousElementSibling) {
                    element = element.previousElementSibling;
                    if (element.tagName === 'H2' || element.tagName === 'H3') {
                        return element.textContent.trim();
                    }
                }
                return '';
            """, table)
        except Exception as e:
            print(f"Error getting category name: {e}")
            return "Unknown Category"

    def scrape_2024(self):
        url = "https://www.baseball-reference.com/teams/split.cgi?t=p&team=COL&year=2024"
        print(f"\nAccessing {url}")
        
        try:
            self.driver.get(url)
            time.sleep(3)
            print("Page loaded, waiting for tables...")
            
            # Wait for tables to load
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "table"))
            )

            # Find all tables
            tables = self.driver.find_elements(By.TAG_NAME, "table")
            print(f"Found {len(tables)} tables")

            # Process each table
            for i, table in enumerate(tables, 1):
                try:
                    print(f"\nProcessing table {i}/{len(tables)}")
                    
                    # Get category name
                    category = self.get_category_name(table)
                    print(f"Category: {category}")
                    
                    # Get table HTML
                    table_html = self.get_table_html(table)
                    if not table_html:
                        print("No table HTML found")
                        continue
                    
                    # Check if it's a game-level table
                    is_game_level = "Game-Level" in table_html
                    
                    # Extract data
                    df = self.extract_table_data(table_html, category)
                    if not df.empty:
                        self.db.store_splits_data(category, df, is_game_level)
                    else:
                        print("No data extracted from table")

                except Exception as e:
                    print(f"Error processing table {i}: {e}")
                    continue

        except Exception as e:
            print(f"Error during scraping: {e}")
            raise

def main():
    base_path = "./rockies_data"
    print(f"Creating directory: {base_path}")
    os.makedirs(base_path, exist_ok=True)
    
    scraper = BaseballReferenceScraper(base_path)
    print("Starting scraper setup...")
    
    try:
        scraper.setup_driver()
        print("\nStarting 2024 season scrape...")
        scraper.scrape_2024()
        print("Scraping complete")
    except Exception as e:
        print(f"Error during execution: {e}")
    finally:
        scraper.cleanup()

if __name__ == "__main__":
    main()