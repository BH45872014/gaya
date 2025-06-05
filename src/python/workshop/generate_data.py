import os
import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
import sqlite3

# Define the database path
db_path = os.path.join(os.path.dirname(__file__), '../../shared/database/financial_data.db')
print(db_path)
# Ensure the directory exists
db_dir = os.path.dirname(db_path)
os.makedirs(db_dir, exist_ok=True)

# Check if the database file exists
if not os.path.exists(db_path):
    print(f"Database file not found. Creating a new database at {db_path}.")
    conn = sqlite3.connect(db_path)
    # Example: Create a sample table (you can modify this as needed)
    conn.execute('''CREATE TABLE IF NOT EXISTS sample_table (
                        id INTEGER PRIMARY KEY,
                        name TEXT NOT NULL,
                        value REAL NOT NULL
                    );''')
    conn.commit()
    conn.close()
    print("Database created successfully.")
else:
    print(f"Database already exists at {db_path}.")

# Configuration
num_books = 200
start_date = datetime(year=2017, month=1, day=2)
end_date = datetime(year=2025, month=6, day=4)
date_range = pd.date_range(start=start_date, end=end_date)
system_entities = ["SAP", "Oracle", "NetSuite", "Dynamics", "QuickBooks"]
system = ["SYSTEM1", "SYSTEM2", "SYSTEM3", "SYSTEM4", "SYSTEM5"]

# Generate SAP books
sapbooks = pd.DataFrame({
    "SAP_BOOK_ID": [f"SAPB{str(i).zfill(4)}" for i in range(1, num_books + 1)],
    "SAP_BOOK_NAME": [f"Book_{i}" for i in range(1, num_books + 1)],
    "COST_CENTER": [f"CC{random.randint(100, 999)}" for _ in range(num_books)],
    "SYSTEM_ENTITY":random.choice(system_entities),
    "SYSTEM":random.choice(system),
    "OPENING_BALANCE": np.random.randint(5_000_000, 10_000_000, num_books)
})

# Save SAP books to CSV
sapbooks.to_csv("sapbooks.csv", index=False)

# Parameters
transaction_types = ['Manual', 'Auto-post', 'Reversal', 'Accrual']
source_systems = ['SAP-FI', 'SAP-CO', 'Manual Entry']
remarks_samples = ['Year-end adjustment', 'Reversal of DOC000123', 'Cost center reallocation', 'Audit correction']

# Generate and write journal entries in batches
document_counter = 1
batch_size = 10000  # Number of rows per batch

with open("journaldata.csv", "w") as journal_file:
    # Write header
    journal_file.write(",".join([
        "SAP_BOOK_ID", "SAP_BOOK_NAME", "COST_CENTER", "TRANSACTION_CURRENCY", "VALUE",
        "ENTRY_DATE", "POSTING_DATE", "USERNAME", "DOCUMENT_NUMBER", "TRANSACTION_TYPE",
        "POSTED_BY", "APPROVED_BY", "CREATED_TIMESTAMP", "UPDATED_TIMESTAMP",
        "SOURCE_SYSTEM", "REMARKS"
    ]) + "\n")

    for _, row in sapbooks.iterrows():
        sap_book_id = row["SAP_BOOK_ID"]
        sap_book_name = row["SAP_BOOK_NAME"]
        cost_center = row["COST_CENTER"]

        for entry_date in date_range:
            num_entries = max(1, int(np.random.normal(loc=75, scale=25)))
            volatility = np.random.normal(loc=1, scale=0.05)

            batch = []
            for _ in range(num_entries):
                value = int(np.random.uniform(-50000, 50000) * volatility)
                created_ts = entry_date + timedelta(
                    hours=random.randint(0, 23),
                    minutes=random.randint(0, 59)
                )
                updated_ts = created_ts + timedelta(minutes=random.randint(1, 90))

                batch.append([
                    sap_book_id, sap_book_name, cost_center, "USD", value,
                    entry_date, entry_date + timedelta(days=random.randint(0, 2)),
                    f"user_{random.randint(1, 50)}",
                    f"DOC{entry_date.strftime('%Y%m%d')}{str(document_counter).zfill(3)}",
                    random.choice(transaction_types),
                    f"user_{random.randint(1, 20)}",
                    None if random.random() < 0.3 else f"manager_{random.randint(1, 5)}",
                    created_ts, updated_ts,
                    random.choice(source_systems),
                    random.choice(remarks_samples)
                ])
                document_counter += 1

            # Write batch to file
            for entry in batch:
                journal_file.write(",".join(map(str, entry)) + "\n")

# Generate and write daily balance snapshots in chunks
chunk_size = 100000  # Number of rows per chunk

# Initialize a dictionary to track balances for each SAP_BOOK_ID
balances = {book_id: sapbooks.loc[sapbooks["SAP_BOOK_ID"] == book_id, "OPENING_BALANCE"].values[0] for book_id in sapbooks["SAP_BOOK_ID"]}

with open("sapbalance.csv", "w") as balance_file:

    # Write header

    balance_file.write(",".join([

        "SAP_BOOK_ID", "DATE", "BALANCE", "DAILY_CHANGE", "TOTAL_JOURNALS", "LAST_UPDATED_BY"

    ]) + "\n")



    # Process journal entries in chunks

    for chunk in pd.read_csv("journaldata.csv", chunksize=100000):

        chunk["ENTRY_DATE"] = pd.to_datetime(chunk["ENTRY_DATE"])



        # Calculate total journals per day per SAP_BOOK_ID

        chunk["TOTAL_JOURNALS"] = chunk.groupby(["SAP_BOOK_ID", "ENTRY_DATE"])["VALUE"].transform("count")



        # Aggregate journal entries by SAP_BOOK_ID and ENTRY_DATE

        aggregated_chunk = chunk.groupby(["SAP_BOOK_ID", "ENTRY_DATE"]).agg({

            "VALUE": "sum",

            "POSTED_BY": "last",

            "TOTAL_JOURNALS": "first"  # Use the precomputed total journals

        }).reset_index()



        # Process each SAP_BOOK_ID

        for book_id in aggregated_chunk["SAP_BOOK_ID"].unique():

            book_data = aggregated_chunk[aggregated_chunk["SAP_BOOK_ID"] == book_id]



            for _, row in book_data.iterrows():

                entry_date = row["ENTRY_DATE"]

                daily_sum = row["VALUE"]

                last_updated_by = row["POSTED_BY"]

                total_journals_for_day = row["TOTAL_JOURNALS"]



                # Update balance

                daily_change = daily_sum

                balances[book_id] += daily_sum



                # Write to file

                balance_file.write(",".join(map(str, [

                    book_id, entry_date, balances[book_id], daily_change, total_journals_for_day, last_updated_by

                ])) + "\n")



print("✅ All data generated in chunks: sapbooks.csv, journaldata.csv, sapbalance.csv")

# Save data to SQLite database
conn = sqlite3.connect("../../shared/database/financial_data.db")
cursor = conn.cursor()

# Create tables
cursor.execute('''
CREATE TABLE IF NOT EXISTS sapbooks (
    SAP_BOOK_ID TEXT PRIMARY KEY,
    SAP_BOOK_NAME TEXT,
    COST_CENTER TEXT,
    SYSTEM_ENTITY TEXT,
    SYSTEM TEXT,
    OPENING_BALANCE INTEGER
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS journaldata (
    SAP_BOOK_ID TEXT,
    SAP_BOOK_NAME TEXT,
    COST_CENTER TEXT,
    TRANSACTION_CURRENCY TEXT,
    VALUE INTEGER,
    ENTRY_DATE TEXT,
    POSTING_DATE TEXT,
    USERNAME TEXT,
    DOCUMENT_NUMBER TEXT PRIMARY KEY,
    TRANSACTION_TYPE TEXT,
    POSTED_BY TEXT,
    APPROVED_BY TEXT,
    CREATED_TIMESTAMP TEXT,
    UPDATED_TIMESTAMP TEXT,
    SOURCE_SYSTEM TEXT,
    REMARKS TEXT
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS sapbalance (
    SAP_BOOK_ID TEXT,
    DATE TEXT,
    BALANCE INTEGER,
    DAILY_CHANGE INTEGER,
    TOTAL_JOURNALS INTEGER,
    LAST_UPDATED_BY TEXT
)
''')

# Load data from CSV files
sapbooks = pd.read_csv("sapbooks.csv")
journaldata = pd.read_csv("journaldata.csv")
sapbalance = pd.read_csv("sapbalance.csv")

# Insert data into tables
sapbooks.to_sql("sapbooks", conn, if_exists="append", index=False)
journaldata.to_sql("journaldata", conn, if_exists="append", index=False)
sapbalance.to_sql("sapbalance", conn, if_exists="append", index=False)

# Commit and close connection
conn.commit()
conn.close()

print("✅ Data successfully loaded into SQLite database.")