import json
import logging
from typing import Optional
import pyodbc
import pandas as pd
import asyncio
from terminal_colors import TerminalColors as tc
from utilities import Utilities

# Define the connection string for SQL Server (update these values as needed)
server = "mitr.database.windows.net"
database = "MITR"
username = "mitradmin"
password = "mitr@1234"
# Define the connection string for SQL Server
# Make sure to update these values with your actual server, database, and authentication details
SQL_SERVER_CONNECTION_STRING = (
    f"Driver={{ODBC Driver 17 for SQL Server}};"
    f"Server={server};"
    f"Database={database};"
    f"Uid={username};"
    f"Pwd={password};"
    f"Encrypt=yes;"
    f"TrustServerCertificate=no;"
    f"Connection Timeout=30;"
)

logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

class FinancialDataSQLServer:
    conn: Optional[pyodbc.Connection]

    def __init__(self: "FinancialDataSQLServer", utilities: Utilities) -> None:
        self.conn = None
        self.utilities = utilities

    async def connect(self: "FinancialDataSQLServer") -> None:
        """Establish a connection to the SQL Server database."""
        try:
            self.conn = pyodbc.connect(SQL_SERVER_CONNECTION_STRING)
            logger.debug("Database connection opened.")
        except Exception as e:
            logger.exception("Error opening database", exc_info=e)
            self.conn = None

    async def close(self: "FinancialDataSQLServer") -> None:
        """Close the database connection."""
        if self.conn:
            self.conn.close()
            logger.debug("Database connection closed.")

    async def _get_table_names(self: "FinancialDataSQLServer") -> list:
        """Get a list of table names in the SQL Server database."""
        return await asyncio.to_thread(self._fetch_table_names)

    def _fetch_table_names(self):
        cursor = self.conn.cursor()
        query = "SELECT table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE';"
        cursor.execute(query)
        tables = cursor.fetchall()
        return [table[0] for table in tables]

    async def _get_column_info(self: "FinancialDataSQLServer", table_name: str) -> list:
        """Get column information for a specific table in SQL Server."""
        return await asyncio.to_thread(self._fetch_column_info, table_name)

    def _fetch_column_info(self, table_name: str):
        cursor = self.conn.cursor()
        query = f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ?"
        cursor.execute(query, (table_name,))
        columns = cursor.fetchall()
        return [f"{col[0]}: {col[1]}" for col in columns]

    async def _get_transaction_types(self: "FinancialDataSQLServer") -> list:
        """Fetch distinct transaction types from the journaldata table."""
        return await asyncio.to_thread(self._fetch_transaction_types)

    def _fetch_transaction_types(self):
        cursor = self.conn.cursor()
        query = "SELECT DISTINCT TRANSACTION_TYPE FROM journaldata;"
        cursor.execute(query)
        result = cursor.fetchall()
        return [row[0] for row in result if row[0] is not None]

    async def _get_currencies(self: "FinancialDataSQLServer") -> list:
        """Fetch distinct transaction currencies from the journaldata table."""
        return await asyncio.to_thread(self._fetch_currencies)

    def _fetch_currencies(self):
        cursor = self.conn.cursor()
        query = "SELECT DISTINCT TRANSACTION_CURRENCY FROM journaldata;"
        cursor.execute(query)
        result = cursor.fetchall()
        return [row[0] for row in result if row[0] is not None]

    async def _get_years(self: "FinancialDataSQLServer") -> list:
        """Fetch distinct years from the journaldata table."""
        return await asyncio.to_thread(self._fetch_years)

    def _fetch_years(self):
        cursor = self.conn.cursor()
        query = "SELECT DISTINCT YEAR(ENTRY_DATE) AS year FROM journaldata ORDER BY year;"
        cursor.execute(query)
        result = cursor.fetchall()
        return [row[0] for row in result if row[0] is not None]

    async def get_database_info(self: "FinancialDataSQLServer") -> str:
        """Get the schema information of the SQL Server database."""
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

        # Fix applied here: Map each item in years list to string
        database_info += f"\nTransaction Types: {', '.join(txn_types)}"
        database_info += f"\nCurrencies: {', '.join(currencies)}"
        database_info += f"\nYears: {', '.join(map(str, years))}"  # Fix applied
        database_info += "\n\n"

        return database_info

    async def async_fetch_data_using_sqlserver_query(self: "FinancialDataSQLServer", sql_query: str) -> str:
        """Execute a raw SQL query and return the result as a JSON string."""
        print(f"\n{tc.BLUE}Function Call: async_fetch_data_using_sql_server_query{tc.RESET}")
        print(f"{tc.BLUE}Executing query: {sql_query}{tc.RESET}\n")

        try:
            result = await asyncio.to_thread(self._fetch_query_result, sql_query)
            return result
        except Exception as e:
            return json.dumps({"SQL query failed": str(e), "query": sql_query})

    def _fetch_query_result(self, sql_query: str):
        cursor = self.conn.cursor()
        cursor.execute(sql_query)
        rows = cursor.fetchall()
        columns = [description[0] for description in cursor.description]

        if not rows:
            return json.dumps("The query returned no results.")
        
        df = pd.DataFrame(rows, columns=columns)
        return df.to_json(index=False, orient="split")
