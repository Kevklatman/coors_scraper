import pandas as pd
import sqlite3

# Connect to the SQLite database
conn = sqlite3.connect('baseball_data.db')

# Create an Excel writer object
writer = pd.ExcelWriter('baseball_data.xlsx', engine='xlsxwriter')

# Retrieve the table names from the database
table_names = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table'", conn)['name'].tolist()

# Iterate over each table and export it to a separate sheet in the Excel file
for table_name in table_names:
    # Read the table data from the database into a DataFrame
    df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
    
    # Write the DataFrame to a sheet in the Excel file
    df.to_excel(writer, sheet_name=table_name, index=False)

# Save the Excel file and close the writer
writer.close()

# Close the database connection
conn.close()