import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

# ---------- CONFIG ----------
DATASET = "billing_export_dataset"
TABLE = "gcp_billing_export_v1_*"
USD_TO_INR = 83.5  # ⚠️ You can update this value based on real-time rates

# ---------- AUTH ----------
@st.cache_resource
def get_bq_client():
    creds_dict = dict(st.secrets["gcp_service_account"])
    project_id = creds_dict["project_id"]
    credentials = service_account.Credentials.from_service_account_info(creds_dict)
    return bigquery.Client(credentials=credentials, project=project_id)

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
    project_id = dict(st.secrets["gcp_service_account"])["project_id"]
    bq_table = f"{project_id}.{DATASET}.{TABLE}"
    query = f"""
        SELECT
            IFNULL(project.name, 'Unknown') AS project,
            IFNULL(service.description, 'Unknown') AS service,
            IFNULL(sku.description, 'Unknown') AS sku,
            usage_start_time,
            cost,
            _PARTITIONTIME AS partition_date
        FROM `{bq_table}`
        WHERE DATE(_PARTITIONTIME) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    """
    return client.query(query).to_dataframe()

# ---------- UI ----------
st.set_page_config(layout="wide")  # 📺 Make UI full-width
st.title("📊 GCP FinOps Dashboard (INR ₹)")

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
            df["cost_inr"] = df["cost"] * USD_TO_INR

            # KPIs
            st.subheader("💰 Total Cost (Last 30 Days)")
            st.metric("Total (INR)", f"₹{df['cost_inr'].sum():,.2f}")

            # Charts
            col1, col2 = st.columns(2)

            with col1:
                st.subheader("📁 Cost by Project")
                st.bar_chart(df.groupby("project")["cost_inr"].sum().sort_values(ascending=False))

            with col2:
                st.subheader("🧱 Cost by Service")
                st.bar_chart(df.groupby("service")["cost_inr"].sum().sort_values(ascending=False))

            st.subheader("📈 Daily Spend Trend")
            st.line_chart(df.groupby("date")["cost_inr"].sum())

            st.subheader("🧾 Billing Data Table")
            st.dataframe(df[["project", "service", "sku", "date", "cost_inr"]].sort_values(by="date", ascending=False).rename(columns={"cost_inr": "cost (INR ₹)"}))

            st.caption("💡 Costs shown in INR (₹). Exchange rate: 1 USD = ₹83.5. Powered by BigQuery + Streamlit 🚀")

    except Exception as e:
        st.error(f"Error fetching data: {e}")
