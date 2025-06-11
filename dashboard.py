import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta

# 📍 Page config (must be first Streamlit command)
st.set_page_config(page_title="GCP FinOps Dashboard", layout="wide")

# 🔐 Load credentials from secrets
credentials = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])
client = bigquery.Client(credentials=credentials, project=credentials.project_id)

# 📅 Filter options
st.sidebar.header("Filter Options")
days = st.sidebar.slider("Days of data to show", 1, 90, 30)
start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

# 🧾 Query billing data
@st.cache_data(ttl=3600)
def load_data():
    query = f"""
    SELECT
      project.id AS project_id,
      service.description AS service,
      sku.description AS sku,
      usage_start_time,
      cost
    FROM `total-apparatus-460306-v7.billing_dataset.gcp_billing_export_v1_*`
    WHERE _PARTITIONTIME >= TIMESTAMP("{start_date}")
      AND cost > 0
    """
    return client.query(query).to_dataframe()

df = load_data()

# 🎯 KPIs
st.title("📊 GCP FinOps Dashboard")
col1, col2 = st.columns(2)
col1.metric("Total Spend (₹)", f"{df['cost'].sum():,.2f}")
col2.metric("Avg Daily Spend (₹)", f"{df.groupby(df['usage_start_time'].dt.date)['cost'].sum().mean():,.2f}")

# 📉 Line Chart: Cost over time
df['date'] = pd.to_datetime(df['usage_start_time']).dt.date
daily_cost = df.groupby('date')['cost'].sum().reset_index()
st.subheader("💹 Daily GCP Spend")
st.plotly_chart(px.line(daily_cost, x='date', y='cost', title="Cost Trend"))

# 🧩 Breakdown by Project
proj_cost = df.groupby('project_id')['cost'].sum().reset_index()
st.subheader("📁 Cost by Project")
st.plotly_chart(px.pie(proj_cost, names='project_id', values='cost', title="Project-wise Spend"))

# 🔎 Optional: Table View
with st.expander("🔍 Raw Billing Data"):
    st.dataframe(df.head(100))
