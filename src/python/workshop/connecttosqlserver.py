import pandas as pd
import pyodbc
 
# Set up the connection details
server = "mitr.database.windows.net"
database = "MITR"
username = "mitradmin"
password = "mitr@1234"
 
# Create the connection string
conn_str = (
    f"Driver={{ODBC Driver 17 for SQL Server}};"
    f"Server={server};"
    f"Database={database};"
    f"Uid={username};"
    f"Pwd={password};"
    f"Encrypt=yes;"
    f"TrustServerCertificate=no;"
    f"Connection Timeout=30;"
)
 
# Connect to the SQL Server
try:
    conn = pyodbc.connect(conn_str)
    print("✅ Connection to SQL Server established successfully.")
except pyodbc.Error as e:
    print(f"❌ Error connecting to SQL Server: {e}")
    exit()
 
# Query the SAPBALANCE table
query = "SELECT * FROM sapbalance"
query_sapooks = "SELECT * FROM sapbooks"
try:
    df = pd.read_sql(query, conn)
    print("✅ Data fetched successfully from SAPBALANCE table.")
    print(df.head())  # Display the first few rows of the data
 
    df_sapbooks = pd.read_sql(query_sapooks, conn)
    print("✅ Data fetched successfully from SAPBOOKS table.")  
    print(df_sapbooks.head())  # Display the first few rows of the data
except Exception as e:
    print(f"❌ Error fetching data: {e}")
 
# Close the connection
conn.close()
print("✅ Connection closed.")