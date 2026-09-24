import os
import time

import pandas as pd
import plotly.express as px
import psycopg2
import streamlit as st


# --------------------------------------------------
# PAGE CONFIGURATION
# --------------------------------------------------

st.set_page_config(
    page_title="Banking Fraud Detection Dashboard",
    page_icon="🚨",
    layout="wide"
)


# --------------------------------------------------
# DATABASE CONFIGURATION
# --------------------------------------------------

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "ecommerce_db",
    "user": "ecommerce_user",
    "password": os.getenv("POSTGRES_PASSWORD")
}


# --------------------------------------------------
# DATABASE CONNECTION
# --------------------------------------------------

def get_connection():
    return psycopg2.connect(**DB_CONFIG)


# --------------------------------------------------
# LOAD TRANSACTIONS
# --------------------------------------------------

@st.cache_data(ttl=5)
def load_transactions():

    query = """
        SELECT
            transaction_id,
            customer_id,
            amount,
            location,
            risk_score,
            fraud_status,
            fraud_reason,
            transaction_time
        FROM transactions
        ORDER BY transaction_time DESC
    """

    conn = get_connection()

    try:
        df = pd.read_sql(query, conn)
    finally:
        conn.close()

    return df


# --------------------------------------------------
# LOAD FRAUD ALERTS
# --------------------------------------------------

@st.cache_data(ttl=5)
def load_fraud_alerts():

    query = """
        SELECT
            transaction_id,
            customer_id,
            risk_score,
            fraud_status,
            fraud_reason,
            alert_time
        FROM fraud_alerts
        ORDER BY alert_time DESC
    """

    conn = get_connection()

    try:
        df = pd.read_sql(query, conn)
    finally:
        conn.close()

    return df


# --------------------------------------------------
# DASHBOARD HEADER
# --------------------------------------------------

st.title("🚨 Real-Time Banking Fraud Detection")
st.caption("Kafka → PySpark → PostgreSQL → Streamlit")


# --------------------------------------------------
# LOAD DATA
# --------------------------------------------------

try:

    transactions = load_transactions()
    fraud_alerts = load_fraud_alerts()

except Exception as e:

    st.error(f"Database connection failed: {e}")
    st.stop()


# --------------------------------------------------
# KPI CALCULATIONS
# --------------------------------------------------

total_transactions = len(transactions)

total_amount = (
    transactions["amount"].sum()
    if not transactions.empty
    else 0
)

fraud_transactions = (
    len(
        transactions[
            transactions["fraud_status"] == "FRAUD"
        ]
    )
    if not transactions.empty
    else 0
)

suspicious_transactions = (
    len(
        transactions[
            transactions["fraud_status"] == "SUSPICIOUS"
        ]
    )
    if not transactions.empty
    else 0
)

fraud_percentage = (
    (fraud_transactions / total_transactions) * 100
    if total_transactions > 0
    else 0
)


# --------------------------------------------------
# KPI CARDS
# --------------------------------------------------

col1, col2, col3, col4, col5 = st.columns(5)

col1.metric(
    "Total Transactions",
    f"{total_transactions:,}"
)

col2.metric(
    "Total Transaction Value",
    f"₹{total_amount:,.2f}"
)

col3.metric(
    "Fraud Transactions",
    f"{fraud_transactions:,}"
)

col4.metric(
    "Suspicious Transactions",
    f"{suspicious_transactions:,}"
)

col5.metric(
    "Fraud Percentage",
    f"{fraud_percentage:.2f}%"
)


st.divider()


# --------------------------------------------------
# CHARTS
# --------------------------------------------------

if not transactions.empty:

    col1, col2 = st.columns(2)

    # ----------------------------------------------
    # TRANSACTIONS OVER TIME
    # ----------------------------------------------

    with col1:

        st.subheader("📈 Transactions Over Time")

        time_df = (
            transactions
            .set_index("transaction_time")
            .resample("1min")
            .size()
            .reset_index(name="transactions")
        )

        fig_time = px.line(
            time_df,
            x="transaction_time",
            y="transactions",
            markers=True,
            title="Transaction Activity"
        )

        fig_time.update_layout(
            xaxis_title="Time",
            yaxis_title="Transactions"
        )

        st.plotly_chart(
            fig_time,
            use_container_width=True
        )

    # ----------------------------------------------
    # FRAUD STATUS
    # ----------------------------------------------

    with col2:

        st.subheader("🥧 Fraud Status Breakdown")

        status_df = (
            transactions["fraud_status"]
            .value_counts()
            .reset_index()
        )

        status_df.columns = [
            "fraud_status",
            "count"
        ]

        fig_status = px.pie(
            status_df,
            names="fraud_status",
            values="count",
            hole=0.4,
            title="Transaction Status"
        )

        st.plotly_chart(
            fig_status,
            use_container_width=True
        )


    # ----------------------------------------------
    # TRANSACTIONS BY LOCATION
    # ----------------------------------------------

    st.subheader("📍 Transactions by Location")

    location_df = (
        transactions["location"]
        .value_counts()
        .reset_index()
    )

    location_df.columns = [
        "location",
        "transactions"
    ]

    fig_location = px.bar(
        location_df,
        x="location",
        y="transactions",
        title="Transactions by Location",
        text_auto=True
    )

    st.plotly_chart(
        fig_location,
        use_container_width=True
    )


    # ----------------------------------------------
    # RECENT TRANSACTIONS
    # ----------------------------------------------

    st.subheader("💳 Recent Transactions")

    display_transactions = transactions.copy()

    display_transactions["amount"] = (
        display_transactions["amount"]
        .map(lambda x: f"₹{x:,.2f}")
    )

    st.dataframe(
        display_transactions.head(20),
        use_container_width=True,
        hide_index=True
    )


else:

    st.info(
        "No transactions found. "
        "Start the Kafka transaction producer."
    )


# --------------------------------------------------
# FRAUD ALERTS
# --------------------------------------------------

st.divider()

st.subheader("🚨 Recent Fraud Alerts")

if not fraud_alerts.empty:

    st.dataframe(
        fraud_alerts.head(20),
        use_container_width=True,
        hide_index=True
    )

else:

    st.success("No fraud alerts detected.")


# --------------------------------------------------
# AUTO REFRESH
# --------------------------------------------------

time.sleep(1)

st.caption(
    "Dashboard refreshes automatically every 5 seconds."
)

st.markdown(
    """
    <script>
        setTimeout(function() {
            window.parent.location.reload();
        }, 5000);
    </script>
    """,
    unsafe_allow_html=True
)