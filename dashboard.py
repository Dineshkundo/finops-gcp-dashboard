import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

# ---------- CONFIG ----------
PROJECT_ID = "total-apparatus-460306-v7"
DATASET = "billing_export_dataset"
TABLE = "gcp_billing_export_v1_*"
BQ_TABLE = f"{PROJECT_ID}.{DATASET}.{TABLE}"

# ---------- AUTH ----------
@st.cache_resource
def get_bq_client():
    creds_dict = st.secrets["gcp_service_account"]  # Load from Streamlit secrets
    credentials = service_account.Credentials.from_service_account_info(creds_dict)
    return bigquery.Client(credentials=credentials, project=PROJECT_ID)

client = get_bq_client()


# ---------- TEST CONNECTION ----------
def test_bq_connection():
    try:
        query = "SELECT CURRENT_DATE() AS today"
        result = client.query(query).result()
        for row in result:
            return f"✅ Connected to BigQuery! Today's date is {row.today}"
    except Exception as e:
        return f"❌ Failed to connect to BigQuery: {e}"

# ---------- BILLING QUERY ----------
@st.cache_data(ttl=600)
def query_billing_data():
    query = f"""
        SELECT
            IFNULL(project.name, 'Unknown') AS project,
            IFNULL(service.description, 'Unknown') AS service,
            IFNULL(sku.description, 'Unknown') AS sku,
            usage_start_time,
            cost,
            _PARTITIONTIME AS partition_date
        FROM `{BQ_TABLE}`
        WHERE DATE(_PARTITIONTIME) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    """
    return client.query(query).to_dataframe()


# ---------- UI ----------
st.title("📊 GCP FinOps Dashboard")

connection_status = test_bq_connection()
if connection_status.startswith("❌"):
    st.error(connection_status)
else:
    st.success(connection_status)

    try:
        df = query_billing_data()

        if df.empty:
            st.warning("No billing data found for the last 30 days.")
        else:
            df["date"] = pd.to_datetime(df["usage_start_time"]).dt.date

            # KPIs
            st.subheader("💰 Total Cost (Last 30 Days)")
            st.metric("Total", f"${df['cost'].sum():,.2f}")

            # Charts
            col1, col2 = st.columns(2)

            with col1:
                st.subheader("📁 Cost by Project")
                st.bar_chart(df.groupby("project")["cost"].sum().sort_values(ascending=False))

            with col2:
                st.subheader("🧱 Cost by Service")
                st.bar_chart(df.groupby("service")["cost"].sum().sort_values(ascending=False))

            st.subheader("📈 Daily Spend Trend")
            st.line_chart(df.groupby("date")["cost"].sum())

            # NEW: Tabular view
            st.subheader("🧾 Billing Data Table")
            st.dataframe(df[["project", "service", "sku", "date", "cost"]].sort_values(by="date", ascending=False))

            st.caption("Powered by BigQuery + Streamlit 🚀")

    except Exception as e:
        st.error(f"Error fetching data: {e}")
