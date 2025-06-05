import json
import logging
from typing import Optional
import aiosqlite
import pandas as pd
from terminal_colors import TerminalColors as tc
from utilities import Utilities

DATA_BASE = "database/financial_data.db"

logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

class FinancialData:
    conn: Optional[aiosqlite.Connection]

    def __init__(self: "FinancialData", utilities: Utilities) -> None:
        self.conn = None
        self.utilities = utilities

    async def connect(self: "FinancialData") -> None:
        db_uri = f"file:{self.utilities.shared_files_path}/{DATA_BASE}?mode=ro"
        try:
            self.conn = await aiosqlite.connect(db_uri, uri=True)
            logger.debug("Database connection opened.")
        except aiosqlite.Error as e:
            logger.exception("Error opening database", exc_info=e)
            self.conn = None

    async def close(self: "FinancialData") -> None:
        if self.conn:
            await self.conn.close()
            logger.debug("Database connection closed.")

    async def _get_table_names(self: "FinancialData") -> list:
        async with self.conn.execute("SELECT name FROM sqlite_master WHERE type='table';") as tables:
            return [table[0] async for table in tables if table[0] != "sqlite_sequence"]

    async def _get_column_info(self: "FinancialData", table_name: str) -> list:
        async with self.conn.execute(f"PRAGMA table_info('{table_name}');") as columns:
            return [f"{col[1]}: {col[2]}" async for col in columns]

    async def _get_transaction_types(self: "FinancialData") -> list:
        query = "SELECT DISTINCT TRANSACTION_TYPE FROM journaldata;"  # Corrected the table name
        async with self.conn.execute(query) as cursor:
            result = await cursor.fetchall()
        return [row[0] for row in result if row[0] is not None]

    async def _get_currencies(self: "FinancialData") -> list:
        query = "SELECT DISTINCT TRANSACTION_CURRENCY FROM journaldata;"  # Corrected the table name
        async with self.conn.execute(query) as cursor:
            result = await cursor.fetchall()
        return [row[0] for row in result if row[0] is not None]

    async def _get_years(self: "FinancialData") -> list:
        query = "SELECT DISTINCT substr(ENTRY_DATE, 1, 4) AS year FROM journaldata ORDER BY year;"  # Corrected the table name
        async with self.conn.execute(query) as cursor:
            result = await cursor.fetchall()
        return [row[0] for row in result if row[0] is not None]

    async def get_database_info(self: "FinancialData") -> str:
        table_dicts = []
        for table_name in await self._get_table_names():
            columns_names = await self._get_column_info(table_name)
            table_dicts.append({"table_name": table_name, "column_names": columns_names})

        database_info = "\n".join(
            [
                f"Table {table['table_name']} Schema: Columns: {', '.join(table['column_names'])}"
                for table in table_dicts
            ]
        )
        txn_types = await self._get_transaction_types()
        currencies = await self._get_currencies()
        years = await self._get_years()

        database_info += f"\nTransaction Types: {', '.join(txn_types)}"
        database_info += f"\nCurrencies: {', '.join(currencies)}"
        database_info += f"\nYears: {', '.join(years)}"
        database_info += "\n\n"

        return database_info

    async def async_fetch_data_using_sqlite_query(self: "FinancialData", sqlite_query: str) -> str:
        print(f"\n{tc.BLUE}Function Call: async_fetch_data_using_sqlite_query{tc.RESET}")
        print(f"{tc.BLUE}Executing query: {sqlite_query}{tc.RESET}\n")

        try:
            async with self.conn.execute(sqlite_query) as cursor:
                rows = await cursor.fetchall()
                columns = [description[0] for description in cursor.description]

            if not rows:
                return json.dumps("The query returned no results.")
            df = pd.DataFrame(rows, columns=columns)
            return df.to_json(index=False, orient="split")

        except Exception as e:
            return json.dumps({"SQLite query failed": str(e), "query": sqlite_query})
