import streamlit as st
st.set_page_config(page_title="GCP FinOps Dashboard", layout="wide")
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

# ----------------------------
# 💱 Use actual invoice rate
# ----------------------------
USD_TO_INR = 85.0338  # From your invoice, May 2025

# ----------------------------
# 🔐 Auth via secrets.toml
# ----------------------------
@st.cache_resource
def get_bq_client():
    creds_dict = dict(st.secrets["gcp_service_account"])
    project_id = creds_dict["project_id"]
    credentials = service_account.Credentials.from_service_account_info(creds_dict)
    return bigquery.Client(credentials=credentials, project=project_id)

client = get_bq_client()

# ----------------------------
# 🔍 Billing Query
# ----------------------------
@st.cache_data(ttl=600)
def query_billing_data():
    project_id = dict(st.secrets["gcp_service_account"])["project_id"]
    dataset = "billing_export_dataset"
    table = "gcp_billing_export_v1_*"
    bq_table = f"{project_id}.{dataset}.{table}"

    query = f"""
        SELECT
            IFNULL(project.name, 'Unknown') AS project,
            IFNULL(service.description, 'Unknown') AS service,
            IFNULL(sku.description, 'Unknown') AS sku,
            usage_start_time,
            cost,
            (SELECT SUM(c.amount) FROM UNNEST(credits) AS c) AS credits_usd,
            cost - (SELECT SUM(c.amount) FROM UNNEST(credits) AS c) AS net_cost_usd,
            _PARTITIONTIME AS partition_date
        FROM `{bq_table}`
        WHERE DATE(_PARTITIONTIME) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    """
    return client.query(query).to_dataframe()

# ----------------------------
# 🎨 Streamlit UI
# ----------------------------
st.title("📊 GCP FinOps Dashboard (INR + Credits)")

try:
    df = query_billing_data()
    if df.empty:
        st.warning("No billing data found for the last 30 days.")
    else:
        # 🧮 Calculations
        df["date"] = pd.to_datetime(df["usage_start_time"]).dt.date
        df["credits_usd"] = df["credits_usd"].fillna(0)
        df["net_cost_usd"] = df["cost"] - df["credits_usd"]
        df["cost_inr"] = df["cost"] * USD_TO_INR
        df["credits_inr"] = df["credits_usd"] * USD_TO_INR
        df["net_cost_inr"] = df["net_cost_usd"] * USD_TO_INR

        # 💰 KPIs
        st.subheader("💰 30-Day Summary (INR)")
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Cost (before credits)", f"₹{df['cost_inr'].sum():,.2f}")
        col2.metric("Credits Used", f"-₹{df['credits_inr'].sum():,.2f}")
        col3.metric("Net Cost", f"₹{df['net_cost_inr'].sum():,.2f}")

        # 📊 Charts
        st.subheader("📁 Cost Breakdown by Project (INR)")
        st.bar_chart(df.groupby("project")["net_cost_inr"].sum().sort_values(ascending=False))

        st.subheader("🧱 Cost Breakdown by Service (INR)")
        st.bar_chart(df.groupby("service")["net_cost_inr"].sum().sort_values(ascending=False))

        st.subheader("📈 Daily Spend Trend (Net INR)")
        st.line_chart(df.groupby("date")["net_cost_inr"].sum())

        # 📋 Tabular view
        st.subheader("🧾 Billing Data Table (INR)")
        st.dataframe(
            df[["date", "project", "service", "sku", "cost_inr", "credits_inr", "net_cost_inr"]]
            .sort_values(by="date", ascending=False)
            .rename(columns={
                "cost_inr": "Cost (INR)",
                "credits_inr": "Credits (INR)",
                "net_cost_inr": "Net Cost (INR)"
            })
        )

        st.caption("Powered by GCP BigQuery + Streamlit 💡")
except Exception as e:
    st.error(f"❌ Error: {e}")
