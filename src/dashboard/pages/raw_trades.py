"""Continuously refreshing view of the latest trades."""

import pandas as pd
import streamlit as st

from database import available_symbols, query_dataframe

st.title("📋 Raw Trades")
st.caption("Latest deduplicated trades stored in PostgreSQL")


@st.fragment(run_every="5s")
def show_raw_trades() -> None:
    try:
        symbols = available_symbols()
        if not symbols:
            st.info("Waiting for live trades.")
            return

        selected_symbol = st.selectbox(
            "Symbol",
            ["All"] + symbols,
            key="raw_trade_symbol",
        )

        if selected_symbol == "All":
            trades = query_dataframe(
                """
                SELECT symbol, price, volume, trade_time, source
                FROM trades
                ORDER BY trade_time DESC
                LIMIT 200
                """
            )
        else:
            trades = query_dataframe(
                """
                SELECT symbol, price, volume, trade_time, source
                FROM trades
                WHERE symbol = %s
                ORDER BY trade_time DESC
                LIMIT 200
                """,
                (selected_symbol,),
            )

        trades["trade_time"] = (
            pd.to_datetime(trades["trade_time"], utc=True)
            .dt.tz_localize(None)
        )
        latest_trade = trades.iloc[0]
        summary = st.columns(4)
        summary[0].metric("Rows displayed", f"{len(trades):,}")
        summary[1].metric("Symbols", trades["symbol"].nunique())
        summary[2].metric(
            "Latest trade",
            f"{latest_trade['symbol']}  ${latest_trade['price']:.2f}",
        )
        summary[3].metric(
            "Last event",
            latest_trade["trade_time"].strftime("%H:%M:%S UTC"),
        )

        st.subheader("Trade feed")
        st.dataframe(
            trades,
            width="stretch",
            hide_index=True,
            column_config={
                "price": st.column_config.NumberColumn(format="$%.2f"),
                "volume": st.column_config.NumberColumn(format="%.2f"),
                "trade_time": st.column_config.DatetimeColumn(
                    "Trade time (UTC)",
                    format="YYYY-MM-DD HH:mm:ss"
                ),
            },
        )
        st.caption("Refreshes every 5 seconds")
    except Exception as error:
        st.error(f"Database unavailable: {error}")


show_raw_trades()
