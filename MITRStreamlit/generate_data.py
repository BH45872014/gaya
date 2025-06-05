
import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

# Configuration
num_books = 200
start_date = datetime(year=2017, month=1, day=2)
end_date = datetime(year=2024, month=9, day=30)
date_range = pd.date_range(start=start_date, end=end_date)

# Generate SAP books
sap_books = pd.DataFrame({
    "SAP_BOOK_ID": [f"SAPB{str(i).zfill(4)}" for i in range(1, num_books + 1)],
    "SAP_BOOK_NAME": [f"Book_{i}" for i in range(1, num_books + 1)],
    "COST_CENTER": [f"CC{random.randint(100, 999)}" for _ in range(num_books)],
    "OPENING_BALANCE": np.random.randint(5_000_000, 10_000_000, num_books)
})

# Parameters
transaction_types = ['Manual', 'Auto-post', 'Reversal', 'Accrual']
source_systems = ['SAP-FI', 'SAP-CO', 'Manual Entry']
remarks_samples = ['Year-end adjustment', 'Reversal of DOC000123', 'Cost center reallocation', 'Audit correction']

# Generate Journal Entries
journal_entries = []
document_counter = 1

for _, row in sap_books.iterrows():
    sap_book_id = row["SAP_BOOK_ID"]
    sap_book_name = row["SAP_BOOK_NAME"]
    cost_center = row["COST_CENTER"]

    for entry_date in date_range:
        num_entries = max(1, int(np.random.normal(loc=75, scale=25)))
        volatility = np.random.normal(loc=1, scale=0.05)

        for _ in range(num_entries):
            value = int(np.random.uniform(-50000, 50000) * volatility)
            created_ts = entry_date + timedelta(
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59)
            )
            updated_ts = created_ts + timedelta(minutes=random.randint(1, 90))

            journal_entries.append({
                "SAP_BOOK_ID": sap_book_id,
                "SAP_BOOK_NAME": sap_book_name,
                "COST_CENTER": cost_center,
                "TRANSACTION_CURRENCY": "USD",
                "VALUE": value,
                "ENTRY_DATE": entry_date,
                "POSTING_DATE": entry_date + timedelta(days=random.randint(0, 2)),
                "USERNAME": f"user_{random.randint(1, 50)}",
                "DOCUMENT_NUMBER": f"DOC{entry_date.strftime('%Y%m%d')}{str(document_counter).zfill(3)}",
                "TRANSACTION_TYPE": random.choice(transaction_types),
                "POSTED_BY": f"user_{random.randint(1, 20)}",
                "APPROVED_BY": None if random.random() < 0.3 else f"manager_{random.randint(1, 5)}",
                "CREATED_TIMESTAMP": created_ts,
                "UPDATED_TIMESTAMP": updated_ts,
                "SOURCE_SYSTEM": random.choice(source_systems),
                "REMARKS": random.choice(remarks_samples)
            })
            document_counter += 1

# Create journal DataFrame
journal_df = pd.DataFrame(journal_entries)

# ----------------------------------------
# Generate Daily Balance Snapshot
# ----------------------------------------
balance_records = []

for book_id in sap_books["SAP_BOOK_ID"]:
    book_journals = journal_df[journal_df["SAP_BOOK_ID"] == book_id].copy()
    book_journals.sort_values("ENTRY_DATE", inplace=True)

    opening_balance = sap_books.loc[sap_books["SAP_BOOK_ID"] == book_id, "OPENING_BALANCE"].values[0]
    current_balance = opening_balance

    for entry_date in date_range:
        daily_journals = book_journals[book_journals["ENTRY_DATE"] == entry_date]
        daily_sum = daily_journals["VALUE"].sum()
        total_journals = len(daily_journals)
        last_updated_by = daily_journals["POSTED_BY"].iloc[-1] if total_journals > 0 else None

        daily_change = daily_sum
        current_balance += daily_sum

        balance_records.append({
            "SAP_BOOK_ID": book_id,
            "DATE": entry_date,
            "BALANCE": current_balance,
            "DAILY_CHANGE": daily_change,
            "TOTAL_JOURNALS": total_journals,
            "LAST_UPDATED_BY": last_updated_by
        })

balance_df = pd.DataFrame(balance_records)

# ----------------------------------------
# Save CSVs
# ----------------------------------------
sap_books.to_csv("sap_books.csv", index=False)
journal_df.to_csv("journal_data.csv", index=False)
sap_books[["SAP_BOOK_ID", "SAP_BOOK_NAME", "COST_CENTER", "OPENING_BALANCE"]].to_csv("balance_data.csv", index=False)
balance_df.to_csv("balance_snapshot.csv", index=False)

print("✅ All data generated: sap_books.csv, journal_data.csv, balance_data.csv, balance_snapshot.csv")
