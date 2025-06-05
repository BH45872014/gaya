import streamlit as st
import pandas as pd
import numpy as np
import openai
import logging
import re

st.set_page_config(page_title="GenAI Hackathon", layout="wide")

# OpenAI API setup for multiple models
openai.api_type = "azure"
#openai.api_base = "https://desai-m2o1yub7-australiaeast.openai.azure.com/"  
openai.api_base = "https://barclays-test.openai.azure.com/"  
openai.api_version = "2024-05-01-preview"  

# Define keys for different models
model_keys = {
    "gpt-35-turbo": "1736284e72144ba99837cbaecb4faa99",
    "gpt-4": "xpHWwMet2gPdNtvuXQd7PB7MnJodiN2pKz15Ga301tu1eue947J3JQQJ99AKACYeBjFXJ3w3AAABACOGSqLD"
}

# Logging setup
logging.basicConfig(filename="app.log", level=logging.INFO, format="%(asctime)s - %(message)s")

# Function to generate September dataset with Balance
def create_september_dataset():
    sap_accounts = [f'ACC{str(i).zfill(3)}' for i in range(1, 11)]
    dates_sept_2023 = pd.date_range(start='2023-09-01', end='2023-09-30')
    balance_sept_2023 = np.random.uniform(10000, 50000, size=(len(dates_sept_2023), len(sap_accounts)))
    df_sept_2023 = pd.DataFrame(balance_sept_2023, columns=sap_accounts, index=dates_sept_2023)
    df_sept_2023 = df_sept_2023.reset_index().melt(id_vars='index', var_name='SAP Account', value_name='Balance')
    df_sept_2023.rename(columns={'index': 'COB Date'}, inplace=True)
    df_sept_2023['Year'] = 2023

    dates_sept_2024 = pd.date_range(start='2024-09-01', end='2024-09-30')
    balance_sept_2024 = np.random.uniform(15000, 60000, size=(len(dates_sept_2024), len(sap_accounts)))
    df_sept_2024 = pd.DataFrame(balance_sept_2024, columns=sap_accounts, index=dates_sept_2024)
    df_sept_2024 = df_sept_2024.reset_index().melt(id_vars='index', var_name='SAP Account', value_name='Balance')
    df_sept_2024.rename(columns={'index': 'COB Date'}, inplace=True)
    df_sept_2024['Year'] = 2024

    return pd.concat([df_sept_2023, df_sept_2024])

# Function to generate Jan-Sep dataset with Balance and Interest Rate
def create_jan_september_dataset():
    sap_accounts = [f'ACC{str(i).zfill(3)}' for i in range(1, 11)]
    dates_2023 = pd.date_range(start='2023-01-01', end='2023-09-30')
    principal_2023 = np.random.uniform(100000, 500000, size=(len(dates_2023), len(sap_accounts)))
    balance_2023 = np.random.uniform(50000, 400000, size=(len(dates_2023), len(sap_accounts)))
    interest_rate_2023 = np.random.uniform(0.01, 0.05, size=(len(dates_2023), len(sap_accounts)))
    df_2023 = pd.DataFrame({
        'COB Date': np.tile(dates_2023, len(sap_accounts)),
        'SAP Account': np.repeat(sap_accounts, len(dates_2023)),
        'Principal Sum': principal_2023.flatten(),
        'Balance': balance_2023.flatten(),
        'Interest Rate': interest_rate_2023.flatten(),
        'Year': 2023
    })

    dates_2024 = pd.date_range(start='2024-01-01', end='2024-09-30')
    principal_2024 = np.random.uniform(150000, 600000, size=(len(dates_2024), len(sap_accounts)))
    balance_2024 = np.random.uniform(70000, 450000, size=(len(dates_2024), len(sap_accounts)))
    interest_rate_2024 = np.random.uniform(0.015, 0.055, size=(len(dates_2024), len(sap_accounts)))
    df_2024 = pd.DataFrame({
        'COB Date': np.tile(dates_2024, len(sap_accounts)),
        'SAP Account': np.repeat(sap_accounts, len(dates_2024)),
        'Principal Sum': principal_2024.flatten(),
        'Balance': balance_2024.flatten(),
        'Interest Rate': interest_rate_2024.flatten(),
        'Year': 2024
    })

    return pd.concat([df_2023, df_2024])

def create_prompt_with_code_request(df_summary, df_detailed):
    df_summary_str = df_summary.head(10).to_string(index=False)
    df_detailed_str = df_detailed.head(10).to_string(index=False)
    prompt = f"""
    I have financial data for multiple SAP accounts for the years 2023 and 2024. The data includes two datasets:
    1. The first dataset (df_summary) contains daily records of balance for each SAP account with a COB Date and Balance Amount. Here is a sample: {df_summary_str}
    2. The second dataset (df_detailed) includes detailed records for each SAP account, with columns for COB Date, Trade Date, Maturity Date, Balance Amount, Floating Rate, Interest Rate, and Principal Amount. Here is a sample: {df_detailed_str}

    Please provide Python code that completes the following tasks and stores the results in the specified variable names:

    1. In `df_summary`, calculate the top 3 SAP accounts with the largest total increase in balance for September and store the result as a list in the variable `top3_increase`. Similarly, calculate the top 3 SAP accounts with the largest decrease in balance and store the result in the variable `top3_decrease`.

    2. Using `df_detailed`, calculate the average balance, principal amount, floating rate, and interest rate for each SAP account for September in both years 2023 and 2024. Ensure that date columns (e.g., COB Date) are converted to datetime format before processing, and verify that numeric columns are in the correct format to avoid errors. 

    3. Calculate the year-over-year (YoY) change from 2023 to 2024 for each SAP account in the columns for average balance, principal amount, floating rate, and interest rate, using the results from step 2. Store these YoY changes in a single DataFrame called `avg_data`, which should have only five columns: `SAP Account`, `Balance_chg`, `Principal_chg`, `FloatingRate_chg`, and `Interest_chg`.

    4. Identify and store the top 3 SAP accounts with the largest YoY increase in floating rate in a list named `top3_floating_increase`, and the top 3 with the largest YoY decrease in floating rate in a list named `top3_floating_decrease`. Similarly, store the top 3 SAP accounts with the largest YoY increase in interest rate in a list named `top3_int_increase` and the top 3 with the largest YoY decrease in interest rate in a list named `top3_int_decrease`.

    Ensure that only the columns specified are included in `avg_data`, with one row per SAP account, and that the YoY calculations are performed correctly. Respond with the code only, without any additional formatting or code block delimiters, so it can be run directly.
    """
    return prompt


def standardize_column_names(df):
    # Standardize column names by stripping whitespace and converting to title case
    df.columns = [col.strip().title() for col in df.columns]
    return df

def get_and_execute_code(prompt, df_september, df_jan_sep, model="gpt-35-turbo"):
    try:
        # Set API key based on model
        openai.api_key = model_keys[model]
        
        # Request code from OpenAI
        response = openai.ChatCompletion.create(
            engine=model,
            messages=[
                {"role": "system", "content": "You are a data analyst who provides Python code."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=800
        )
        
        # Extract Python code from response
        code = response['choices'][0]['message']['content']
        st.subheader("Generated Python Code")
        st.code(code, language='python')
        
        # Clean up the code using regex to remove markdown formatting and unexpected characters
        code_match = re.search(r"```python(.*?)```", code, re.DOTALL)
        if code_match:
            code_to_execute = code_match.group(1).strip()
        else:
            code_to_execute = code.strip()
        
        # Log the code execution
        logging.info("Executing the following code:\n%s", code_to_execute)

        # Execute the sanitized Python code and capture the output
        local_vars = {"df_september": df_september, "df_jan_sep": df_jan_sep}
        exec(code_to_execute, globals(), local_vars)
        
        # Display results
        result_vars = [
            'top3_increase', 'top3_decrease', 'avg_sept', 
            'top3_bal_increase', 'top3_bal_decrease', 
            'top3_int_increase', 'top3_int_decrease'
        ]
        
        for var in result_vars:
            if var in local_vars:
                st.subheader(f"{var.replace('_', ' ').title()}")
                st.write(local_vars[var])
        
        if not any(var in local_vars for var in result_vars):
            st.error("The generated code did not produce the expected variables for display.")
    
    except Exception as e:
        logging.error("An error occurred during code execution: %s", e)
        st.error(f"An error occurred: {e}")

def get_movement_commentary(selected_account, avg_sept):
    # Generate a prompt for OpenAI to provide commentary
    prompt = f"""
    The average balance and interest rate data for SAP accounts for September 2023 and 2024 are as follows:
    {avg_sept.to_string(index=False)}

    Analyze the data and provide a detailed commentary on why there was a movement in the balance and interest rate for the SAP account '{selected_account}' from 2023 to 2024.
    Explain potential reasons for the observed changes in balance and interest rate.
    """

    try:
        response = openai.ChatCompletion.create(
            engine="gpt-35-turbo",
            messages=[
                {"role": "system", "content": "You are a financial data analyst who provides detailed commentaries."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=800
        )
        commentary = response['choices'][0]['message']['content']
        return commentary
    except Exception as e:
        logging.error("An error occurred while generating commentary: %s", e)
        return "An Error has occurred" 

# Streamlit UI
st.title("GenAI Hackathon - SAP Accounts Analysis")
st.write("Analyze SAP accounts data using GenAI with either uploaded or generated data. Choose an option below:")

# User option to upload files or generate data
data_option = st.radio("Choose Data Option", ["Upload Files", "Generate Sample Data"], horizontal=True)

if data_option == "Upload Files":
    uploaded_file_sept = st.file_uploader("Upload Summary Dataset", type=["csv"])
    uploaded_file_jan_sep = st.file_uploader("Upload Detailed Dataset", type=["csv"])
    if uploaded_file_sept and uploaded_file_jan_sep:
        df_september = pd.read_csv(uploaded_file_sept)
        df_jan_sep = pd.read_csv(uploaded_file_jan_sep)
        st.success("Both datasets uploaded successfully!")
else:
    df_september = create_september_dataset()
    df_jan_sep = create_jan_september_dataset()
    st.success("Sample data generated successfully!")

# Model selection dropdown
model_option = st.selectbox("Select Model for Analysis", ["gpt-35-turbo", "gpt-4"])

# Load or create datasets
df_september = standardize_column_names(create_september_dataset())
df_jan_sep = standardize_column_names(create_jan_september_dataset())

# Check column names for debugging and logging
logging.info("df_september columns: %s", df_september.columns)
logging.info("df_jan_sep columns: %s", df_jan_sep.columns)

# Display data if available
if 'df_september' in locals() and 'df_jan_sep' in locals():
    with st.expander("View Summary Dataset", expanded=True):
        st.write(df_september.head(10))
    with st.expander("View Detailed Dataset", expanded=True):
        st.write(df_jan_sep.head(10))

    # Create and display prompt
    prompt = create_prompt_with_code_request(df_september, df_jan_sep)
    with st.expander("View Prompt Questions", expanded=True):
        user_prompt = st.text_area("Prompt for Python Code", value=prompt, height=200)

    
    # Get and execute code from OpenAI
    if st.button("Get Python Code and Execute"):
        get_and_execute_code(prompt, df_september, df_jan_sep, model=model_option)

# Button for commentary on balance movement
st.subheader("Request Commentary on Balance Movement")
selected_account = st.selectbox("Select an SAP Account for commentary:", df1['SAP_Account'].unique())
if st.button("Get Commentary"):
    commentary_prompt = f"Provide a detailed commentary on the movement of balance, average floating rate, and average interest rate for SAP Account {selected_account}."
    commentary_response = openai.ChatCompletion.create(
        model="gpt-35-turbo",
        messages=[{"role": "user", "content": commentary_prompt}]
    )
    st.subheader("Commentary from OpenAI")
    st.write(commentary_response['choices'][0]['message']['content'])
