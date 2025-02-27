"""
Streamlit Dashboard for Snowflake Data Visualization

This script retrieves financial data from Snowflake, processes it using SQL queries,
and displays the results in an interactive Streamlit app. The dashboard includes
various visualizations and tabular data for insights into stock positions and trends.

Dependencies:
- streamlit
- snowflake
- pandas
- plotly.express
- st_aggrid
- snowflake.snowpark
"""

# Import python packages
import streamlit as st
import snowflake
import pandas as pd
import plotly.express as px
from st_aggrid import AgGrid, GridOptionsBuilder
from snowflake.snowpark.context import get_active_session

# Get the current credentials
session = get_active_session()

@st.cache_data
def fetch_data(query):
    """
    Executes an SQL query in Snowflake and returns the result as a Pandas DataFrame.

    Args:
        query (str): SQL query string.

    Returns:
        pd.DataFrame: Query results in a Pandas DataFrame.
    """
    if not isinstance(query, str):
        raise ValueError("Query must be a string.")
    try:
        return session.sql(query).to_pandas()
    except Exception as e:
        st.error(f"Error executing query: {e}")
        return pd.DataFrame()

def build_qry(tup):
    """
    Constructs a SQL subquery with an alias.

    Args:
        tup (tuple): A tuple containing the alias and query string.

    Returns:
        str: Formatted SQL subquery.
    """
    if not isinstance(tup, tuple) or len(tup) != 2:
        raise ValueError("Input must be a tuple with two elements: (alias, query).")
    name, query = tup
    return f"{name} AS ({query})"

def build_seq_qrys(list_queries):
    """
    Builds a sequence of SQL subqueries.

    Args:
        list_queries (list): A list of tuples containing alias and query strings.

    Returns:
        str: A formatted SQL sequence.
    """
    if not isinstance(list_queries, list) or not all(isinstance(i, tuple) and len(i) == 2 for i in list_queries):
        raise ValueError("Input must be a list of tuples with two elements each: (alias, query).")
    return ",".join([build_qry(_) for _ in list_queries])

def build_CTE(list_queries, return_qry):
    """
    Constructs a Common Table Expression (CTE) query.

    Args:
        list_queries (list): List of tuples defining CTE subqueries.
        return_qry (str): The main query that follows the CTE definitions.

    Returns:
        str: A complete CTE SQL query.
    """
    if not isinstance(return_qry, str):
        raise ValueError("return_qry must be a string.")
    seq_qrys = build_seq_qrys(list_queries)
    return f"WITH {seq_qrys} {return_qry};"

def get_query_result(query_list, result_query):
    """Fetch and return data from a dynamically built CTE query."""
    return fetch_data(build_CTE(query_list, result_query))

def show_grid(df):
    """
    Displays a paginated DataFrame in Streamlit.

    Args:
        df (pd.DataFrame): The DataFrame to display.
    """
    if not isinstance(df, pd.DataFrame):
        raise ValueError("Input must be a Pandas DataFrame.")
    # Pagination Setup
    page_size = 5  # Number of rows per page
    total_pages = len(df) // page_size
    
    # Select Page
    page_number = st.number_input("Page Number", min_value=1, max_value=total_pages+1, step=1, value=1)
    
    # Display Paginated Data
    start_idx = (page_number - 1) * page_size
    end_idx = start_idx + page_size
    st.dataframe(df.iloc[start_idx:end_idx])

# SQL Queries for data cleaning and processing
def show_result(df, title):
    """
    Displays a DataFrame with a title in Streamlit.

    Args:
        df (pd.DataFrame): The DataFrame to display.
        title (str): The title of the section.
    """
    if not isinstance(df, pd.DataFrame):
        raise ValueError("df must be a Pandas DataFrame.")
    if not isinstance(title, str):
        raise ValueError("title must be a string.")
    st.write(title)
    show_grid(df)

def cache_df(df_name, df):
    """
    Caches a DataFrame in Streamlit's session state.

    Args:
        df_name (str): The name under which the DataFrame will be stored.
        df (pd.DataFrame): The DataFrame to cache.

    Raises:
        ValueError: If df_name is not a string or df is not a Pandas DataFrame.
        Exception: If an error occurs during caching.
    """
    if not isinstance(df_name, str):
        raise ValueError("df_name must be a string.")
    if not isinstance(df, pd.DataFrame):
        raise ValueError("df must be a Pandas DataFrame.")
    
    try:
        if df_name not in st.session_state:
            st.session_state[df_name] = df
    except Exception as e:
        logging.error(f"Error caching DataFrame {df_name}: {e}")
        st.error("An error occurred while caching the DataFrame.")


# Filters the COMPANY, PRICE and POSITION tables to remove NULL entries.
# Ensures that only valid records with essential information are considered.
TEMP_CLEAN_COMPANY = """
    SELECT DISTINCT *
    FROM COMPANY C
    WHERE 
        C.ID IS NOT NULL AND
        C.SECTOR_NAME IS NOT NULL AND
        C.TICKER IS NOT NULL
"""

TEMP_CLEAN_PRICE = """
    SELECT DISTINCT P.CLOSE_USD, P.COMPANY_ID, P.DATE
    FROM PRICE P
    WHERE
        P.CLOSE_USD IS NOT NULL AND
        P.DATE IS NOT NULL AND
        P.COMPANY_ID IS NOT NULL
"""

TEMP_CLEAN_POSITION = """
    SELECT DISTINCT *
    FROM POSITION PO
    WHERE
        PO.COMPANY_ID IS NOT NULL AND
        PO.DATE IS NOT NULL AND
        PO.SHARES IS NOT NULL
"""

# Join cleaned tables COMPANY, PRICE and POSITION and hendle missing files using window function.
# This ensures that if a daily price is missing, the most recent non-null closing price is used.
TEMP_CLOSE_USD_CLEAN = """
    SELECT 
        C.TICKER,
        C.SECTOR_NAME,
        PO.COMPANY_ID,
        PO.SHARES,
        P.CLOSE_USD,
        PO.DATE,
        LAST_VALUE(P.CLOSE_USD IGNORE NULLS) 
        OVER (PARTITION BY PO.COMPANY_ID ORDER BY PO.DATE ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS LATEST_CLOSE_USD
    FROM TEMP_CLEAN_COMPANY C
    INNER JOIN TEMP_CLEAN_POSITION PO ON PO.COMPANY_ID = C.ID
    LEFT JOIN TEMP_CLEAN_PRICE P ON P.COMPANY_ID = PO.COMPANY_ID AND P.DATE = PO.DATE
"""

# Final calculation. Computes the daily position in USD.
# Use valid price multiplied by SHARES to compute the total position value in USD.
TEMP_CLOSE_USD_POSITIONS = """
    SELECT 
        TICKER, COMPANY_ID, DATE, SECTOR_NAME,
        ROUND(SHARES * COALESCE(CLOSE_USD, LATEST_CLOSE_USD), 2) AS CLOSE_USD_POSITION
    FROM TEMP_CLOSE_USD_CLEAN
    ORDER BY DATE DESC
"""

# Fetch and display results for Questions 1    
question1_list_qrys = [
    ("TEMP_CLEAN_COMPANY",TEMP_CLEAN_COMPANY),
    ("TEMP_CLEAN_PRICE",TEMP_CLEAN_PRICE),
    ("TEMP_CLEAN_POSITION", TEMP_CLEAN_POSITION),
    ("TEMP_CLOSE_USD_CLEAN", TEMP_CLOSE_USD_CLEAN)
]

# Caching the result of the first question.
cache_df("df_question_1", get_query_result(question1_list_qrys, TEMP_CLOSE_USD_POSITIONS))

# Filters TEMP_CLOSE_USD_POSITIONS to retain only the data from the past 1 year.
TEMP_CLOSE_USD_POSITIONS_LAST_YEAR = """
    SELECT CP.TICKER, CP.COMPANY_ID, CP.SECTOR_NAME, CP.DATE, CP.CLOSE_USD_POSITION  
    FROM TEMP_CLOSE_USD_POSITIONS CP
    WHERE DATE >= DATEADD(YEAR, -1, CURRENT_DATE)
"""

# Computes the average position in USD 
TEMP_AVG_POSITIONS_BY_COMPANY = """
    SELECT TICKER, ROUND(AVG(CLOSE_USD_POSITION),2) AS AVER, ROW_NUMBER() OVER (ORDER BY AVER DESC) AS rn
    FROM TEMP_CLOSE_USD_POSITIONS_LAST_YEAR
    GROUP BY TICKER
"""

# Counts the total number of companies
TEMP_NUMROWS = "SELECT COUNT(*) AS cnt FROM TEMP_AVG_POSITIONS_BY_COMPANY"

# Extracts only the top 25% of ranked companies.
TEMP_RANKED_COMPANIES = """
    SELECT TICKER, AVER
    FROM TEMP_AVG_POSITIONS_BY_COMPANY
    WHERE rn <= (SELECT FLOOR(cnt * 0.25) FROM TEMP_NUMROWS)
"""

# Fetch and display results for Questions 2
question2_list_qrys = question1_list_qrys + [
    ("TEMP_CLOSE_USD_POSITIONS", TEMP_CLOSE_USD_POSITIONS),
    ("TEMP_CLOSE_USD_POSITIONS_LAST_YEAR", TEMP_CLOSE_USD_POSITIONS_LAST_YEAR),
    ("TEMP_AVG_POSITIONS_BY_COMPANY", TEMP_AVG_POSITIONS_BY_COMPANY),
    ("TEMP_NUMROWS", TEMP_NUMROWS)
]

# Caching the result of the second question.
cache_df("df_question_2", get_query_result(question2_list_qrys, TEMP_RANKED_COMPANIES))

# Fetch and display results for Questions 3
question3_list_qrys = question1_list_qrys + [
    ("TEMP_CLOSE_USD_POSITIONS", TEMP_CLOSE_USD_POSITIONS)
]

# Calculates the daily sector position in USD by summing up the total USD position of all companies in each sector. 
question3_result_qry = """
    SELECT SECTOR_NAME, DATE, ROUND(SUM(CLOSE_USD_POSITION), 2) AS USD_POSITION 
    FROM TEMP_CLOSE_USD_POSITIONS
    GROUP BY SECTOR_NAME, DATE
    ORDER BY DATE DESC
"""

# Caching the result of the third question.
cache_df("df_question_3", get_query_result(question3_list_qrys, question3_result_qry))

# Visualization Widget 1
df_question_3 = st.session_state["df_question_3"]
if df_question_3.empty:
    st.warning("No sector position data available.")
else:
    most_recent_date = df_question_3["DATE"].max()
df_filtered = df_question_3[df_question_3["DATE"] == most_recent_date].sort_values(by="USD_POSITION", ascending=False).head(10)

# Display Chart
st.write(f"### Top 10 Sectors by Position on {most_recent_date}")

fig = px.bar(df_filtered, x="USD_POSITION", y="SECTOR_NAME", orientation="h", 
             title="Top 10 Sectors by USD Position",
             labels={"USD_POSITION": "Position (USD)", "SECTOR_NAME": "Sector"},
             text_auto=True)

# Move text labels outside the bars
fig.update_traces(textposition="outside")

st.plotly_chart(fig)

# Visualization Widget 2
# Display in Streamlit
st.write("### Top 25% Companies by Average Position (Last Year)")
show_grid(st.session_state["df_question_2"])

# Visualization Widget 3
#Create selectbox for company selection
df_question_1 = st.session_state["df_question_1"]
if df_question_1.empty:
    st.warning("No company position data available.")
else:
    companies = list(df_question_1["TICKER"].unique())
selected_company = st.selectbox("Select a Company:", companies)

# Fetch data for selected company
#df = get_company_data(selected_company)
df = df_question_1[df_question_1["TICKER"]==selected_company]

# Display the chart
st.write(f"### Daily Close Price for {selected_company}")

fig = px.line(df, x="DATE", y="CLOSE_USD_POSITION", title=f"Daily Close Price of {selected_company}", labels={"CLOSE_USD_POSITION": "Close Price (USD)"},render_mode="svg")
st.plotly_chart(fig)
