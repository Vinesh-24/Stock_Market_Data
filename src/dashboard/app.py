"""Entry point for the stock market Streamlit dashboard."""

from datetime import timezone

import streamlit as st

from database import database_status

st.set_page_config(
    page_title="Stock Market Live",
    page_icon="📈",
    layout="wide",
)

st.markdown(
    """
    <style>
    .stApp {
        background:
            radial-gradient(circle at top right, #e8f5ee 0, transparent 28%),
            #f7f9fc;
    }
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #102a43 0%, #193b5a 100%);
    }
    [data-testid="stSidebar"] * {
        color: #f8fafc;
    }
    [data-testid="stMetric"] {
        background: white;
        border: 1px solid #e2e8f0;
        border-radius: 12px;
        padding: 16px;
        box-shadow: 0 4px 12px rgba(15, 23, 42, 0.05);
    }
    [data-testid="stDataFrame"] {
        border: 1px solid #e2e8f0;
        border-radius: 12px;
        overflow: hidden;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

with st.sidebar:
    st.markdown("## 📈 Stock Market Live")
    st.caption("Finnhub → Kafka → PostgreSQL")


@st.fragment(run_every="10s")
def show_database_health() -> None:
    healthy, latest_received = database_status()
    with st.sidebar:
        if healthy:
            st.success("PostgreSQL connected")
            if latest_received is not None:
                latest_utc = latest_received.astimezone(timezone.utc)
                st.caption(
                    "Latest event: "
                    f"{latest_utc.strftime('%Y-%m-%d %H:%M:%S')} UTC"
                )
        else:
            st.error("PostgreSQL unavailable")


show_database_health()

navigation = st.navigation(
    [
        st.Page(
            "pages/raw_trades.py",
            title="Raw Trades",
            icon=":material/receipt_long:",
            default=True,
        ),
        st.Page(
            "pages/live_metrics.py",
            title="Live Metrics",
            icon=":material/monitoring:",
        ),
    ]
)
navigation.run()
