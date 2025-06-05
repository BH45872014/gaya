import random
import csv
import sqlite3
from datetime import datetime, timedelta
from collections import defaultdict

# === File Names and DB Name ===
BOOKS_CSV = "books.csv"
JOURNALS_CSV = "journal_entries.csv"
BALANCES_CSV = "balance_records.csv"
SQLITE_DB = "financial_data.db"

# === Step 1: Generate Book Records ===
book_records = []
book_name_samples = ["General Ledger", "Accounts Payable", "Accounts Receivable", "Fixed Assets", "Inventory"]
system_entities = ["SAP", "Oracle", "NetSuite", "Dynamics", "QuickBooks"]

for i in range(1, 101):
    book_id = f"B{str(i).zfill(4)}"
    book_name = random.choice(book_name_samples) + f" {random.randint(1, 10)}"
    system_entity = random.choice(system_entities)
    book_records.append({
        "SAP_BOOK_ID": book_id,
        "Bookname": book_name,
        "systementity": system_entity
    })

# === Step 2: Generate Journal Entries ===
journal_entries = []
transaction_types = ["Credit", "Debit", "Transfer"]
remarks_samples = ["Monthly accrual", "Correction", "Initial entry", "Reclassification"]
document_counter = 1

dates_list = [datetime.now() - timedelta(days=i) for i in range(1000)]

for entry_date_dt in dates_list:
    entry_date = entry_date_dt.strftime('%Y%m%d')
    for _ in range(1000):  # 1,000 entries per date
        book = random.choice(book_records)
        posting_date_dt = entry_date_dt + timedelta(days=random.randint(0, 3))
        posting_date = posting_date_dt.strftime('%Y%m%d')
        value = round(random.uniform(100, 10000), 2)
        approved_by = None if random.random() < 0.3 else f"manager_{random.randint(1, 5)}"

        journal_entries.append({
            "DOCUMENT_NUMBER": f"DOC{entry_date}{str(document_counter).zfill(6)}",
            "SAP_BOOK_ID": book["SAP_BOOK_ID"],
            "SAP_BOOK_NAME": book["Bookname"],
            "COST_CENTER": f"CC{random.randint(1000, 9999)}",
            "TRANSACTION_CURRENCY": "USD",
            "VALUE": value,
            "ENTRY_DATE": entry_date,
            "POSTING_DATE": posting_date,
            "USERNAME": f"user_{random.randint(1, 50)}",
            "TRANSACTION_TYPE": random.choice(transaction_types),
            "POSTED_BY": f"user_{random.randint(1, 20)}",
            "APPROVED_BY": approved_by,
            "CREATED_TIMESTAMP": datetime.now().isoformat(),
            "UPDATED_TIMESTAMP": datetime.now().isoformat(),
            "SOURCE_SYSTEM": book["systementity"],
            "REMARKS": random.choice(remarks_samples)
        })
        document_counter += 1

# === Step 3: Generate Balance Records by Book & Date ===
balance_data = defaultdict(lambda: {"total_value": 0, "count": 0})

for entry in journal_entries:
    key = (entry["SAP_BOOK_ID"], entry["ENTRY_DATE"])
    balance_data[key]["total_value"] += entry["VALUE"]
    balance_data[key]["count"] += 1

balance_records = []
for (book_id, entry_date), stats in balance_data.items():
    total = stats["total_value"]
    count = stats["count"]
    daily_change = round(total / max(count, 1), 2)
    balance_records.append({
        "SAP_BOOK_ID": book_id,
        "DATE": entry_date,
        "TOTAL_JOURNALS": count,
        "LAST_UPDATED_BY": f"user_{random.randint(1, 10)}",
        "BALANCE": round(total, 2),
        "DAILY_CHANGE": daily_change
    })

# === Step 4: Write CSV files ===

def write_csv(filename, data):
    with open(filename, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)

write_csv(BOOKS_CSV, book_records)
write_csv(JOURNALS_CSV, journal_entries)
write_csv(BALANCES_CSV, balance_records)

print("✅ CSV files written.")

# === Step 5: Write to SQLite ===

conn = sqlite3.connect(SQLITE_DB)
c = conn.cursor()

# Create tables
c.execute("""
CREATE TABLE IF NOT EXISTS books (
    SAP_BOOK_ID TEXT PRIMARY KEY,
    Bookname TEXT,
    systementity TEXT
)
""")

c.execute("""
CREATE TABLE IF NOT EXISTS journal_entries (
    DOCUMENT_NUMBER TEXT PRIMARY KEY,
    SAP_BOOK_ID TEXT,
    SAP_BOOK_NAME TEXT,
    COST_CENTER TEXT,
    TRANSACTION_CURRENCY TEXT,
    VALUE REAL,
    ENTRY_DATE TEXT,
    POSTING_DATE TEXT,
    USERNAME TEXT,
    TRANSACTION_TYPE TEXT,
    POSTED_BY TEXT,
    APPROVED_BY TEXT,
    CREATED_TIMESTAMP TEXT,
    UPDATED_TIMESTAMP TEXT,
    SOURCE_SYSTEM TEXT,
    REMARKS TEXT,
    FOREIGN KEY(SAP_BOOK_ID) REFERENCES books(SAP_BOOK_ID)
)
""")

c.execute("""
CREATE TABLE IF NOT EXISTS balance_records (
    SAP_BOOK_ID TEXT,
    DATE TEXT,
    TOTAL_JOURNALS INTEGER,
    LAST_UPDATED_BY TEXT,
    BALANCE REAL,
    DAILY_CHANGE REAL,
    PRIMARY KEY (SAP_BOOK_ID, DATE),
    FOREIGN KEY(SAP_BOOK_ID) REFERENCES books(SAP_BOOK_ID)
)
""")

# Insert data into tables with executemany for performance
def insert_many(table, data):
    keys = data[0].keys()
    placeholders = ','.join('?' for _ in keys)
    query = f"INSERT OR REPLACE INTO {table} ({','.join(keys)}) VALUES ({placeholders})"
    values = [tuple(d[k] for k in keys) for d in data]
    c.executemany(query, values)

insert_many("books", book_records)
print("✅ Books inserted into SQLite.")

insert_many("journal_entries", journal_entries)
print("✅ Journal entries inserted into SQLite.")

insert_many("balance_records", balance_records)
print("✅ Balance records inserted into SQLite.")

conn.commit()
conn.close()

print(f"✅ All data saved in SQLite DB '{SQLITE_DB}' and CSV files.")
