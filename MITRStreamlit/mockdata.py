import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Settings
num_sap_accounts = 1000
cob_dates = ["2023-09-29", "2024-09-30"]
trade_start_date = datetime(2023, 1, 1)
trade_end_date = datetime(2024, 9, 30)

# Generate unique Trade IDs for each day for each SAP Account
trade_dates = pd.date_range(trade_start_date, trade_end_date)
np.random.seed(42)

# Create DataFrame for all combinations
data = []

for cob_date in cob_dates:
    for sap_account in [f"ACC-{i+1:04d}" for i in range(num_sap_accounts)]:
        for trade_date in trade_dates:
            num_trades_per_day = np.random.randint(1, 4)  # 1 to 3 trades per day
            for _ in range(num_trades_per_day):
                trade_id = f"{sap_account}-{trade_date.strftime('%Y%m%d')}-{np.random.randint(1000, 9999)}"
                
                # Randomly set maturity to the next day or later
                maturity_date = trade_date + timedelta(days=np.random.randint(1, 30))
                
                principal_amount = np.round(np.random.uniform(1, 10), 2) * 1e6
                balance_amount = np.round(np.random.uniform(20000, 99000), 2)
                interest_rate = np.round(np.random.uniform(4.5, 5.8), 2)
                floating_rate = np.round(np.random.uniform(4.0, 6.0), 2)

                data.append([
                    cob_date,
                    sap_account,
                    floating_rate,
                    interest_rate,
                    balance_amount,
                    trade_date,
                    maturity_date,
                    trade_id,
                    principal_amount
                ])

# Create DataFrame
columns = [
    "COB Date",
    "SAP Account",
    "Floating Rate",
    "Interest Rate",
    "Balance Amount",
    "Trade Date",
    "Maturity Date",
    "Trade ID",
    "Principal/Notional Amount"
]
df = pd.DataFrame(data, columns=columns)

# Display a sample of the data
df.head(10)
