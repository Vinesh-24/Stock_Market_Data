"""Continuously refreshing one-minute market metrics."""

import altair as alt
import pandas as pd
import streamlit as st

from database import available_symbols, query_dataframe

st.title("📊 Live Metrics")
st.caption("One-minute metrics calculated from deduplicated trades")


@st.fragment(run_every="5s")
def show_live_metrics() -> None:
    try:
        symbols = available_symbols()
        if not symbols:
            st.info("Waiting for live metrics.")
            return

        selected_symbol = st.selectbox(
            "Symbol",
            symbols,
            key="metric_symbol",
        )
        metrics = query_dataframe(
            """
            SELECT
                minute,
                open_price,
                high_price,
                low_price,
                close_price,
                volume,
                trade_count,
                vwap,
                price_change,
                moving_average_5,
                moving_average_15,
                rolling_volatility_15
            FROM one_minute_metrics
            WHERE symbol = %s
              AND minute >= (
                  SELECT date_trunc('day', MAX(minute))
                  FROM one_minute_metrics
                  WHERE symbol = %s
              )
            ORDER BY minute DESC
            LIMIT 120
            """,
            (selected_symbol, selected_symbol),
        ).sort_values("minute")
        metrics["minute"] = (
            pd.to_datetime(metrics["minute"], utc=True)
            .dt.tz_localize(None)
        )
        metrics["segment"] = (
            metrics["minute"].diff().gt(pd.Timedelta(minutes=1)).cumsum()
        )

        if metrics.empty:
            st.info("Waiting for live metrics.")
            return

        latest = metrics.iloc[-1]
        st.caption(
            "Latest completed minute: "
            f"{latest['minute'].strftime('%Y-%m-%d %H:%M UTC')}"
        )

        columns = st.columns(6)
        columns[0].metric(
            "Close",
            f"${latest['close_price']:.2f}",
            f"{latest['price_change']:.2f}",
        )
        columns[1].metric("High", f"${latest['high_price']:.2f}")
        columns[2].metric("Low", f"${latest['low_price']:.2f}")
        columns[3].metric("Volume", f"{latest['volume']:,.0f}")
        columns[4].metric("VWAP", f"${latest['vwap']:.2f}")
        columns[5].metric("Trades", f"{latest['trade_count']:,.0f}")

        display_metrics = (
            metrics.drop(columns="segment")
            .sort_values("minute", ascending=False)
            .copy()
        )

        chart_tab, table_tab = st.tabs(["Price trends", "Metrics table"])
        with chart_tab:
            st.subheader("Close price and moving averages (UTC)")
            chart_data = metrics[
                [
                    "minute",
                    "segment",
                    "close_price",
                    "moving_average_5",
                    "moving_average_15",
                ]
            ].melt(
                id_vars=["minute", "segment"],
                var_name="metric",
                value_name="price",
            )
            price_chart = (
                alt.Chart(chart_data)
                .mark_line(strokeWidth=2)
                .encode(
                    x=alt.X(
                        "minute:T",
                        axis=alt.Axis(
                            format="%H:%M",
                            title="Time (UTC)",
                            tickMinStep=60_000,
                        ),
                    ),
                    y=alt.Y(
                        "price:Q",
                        title="Price (USD)",
                        scale=alt.Scale(zero=False),
                    ),
                    color=alt.Color("metric:N", title="Metric"),
                    detail=alt.Detail("segment:N"),
                    tooltip=[
                        alt.Tooltip(
                            "minute:T",
                            title="Minute (UTC)",
                            format="%Y-%m-%d %H:%M",
                        ),
                        alt.Tooltip("metric:N", title="Metric"),
                        alt.Tooltip("price:Q", title="Price", format=".2f"),
                    ],
                )
            )
            st.altair_chart(price_chart, width="stretch")

            st.subheader("One-minute volume")
            volume_chart = (
                alt.Chart(metrics)
                .mark_bar(color="#16A34A")
                .encode(
                    x=alt.X(
                        "minute:T",
                        axis=alt.Axis(
                            format="%H:%M",
                            title="Time (UTC)",
                            tickMinStep=60_000,
                        ),
                    ),
                    y=alt.Y("volume:Q", title="Volume"),
                    tooltip=[
                        alt.Tooltip(
                            "minute:T",
                            title="Minute (UTC)",
                            format="%Y-%m-%d %H:%M",
                        ),
                        alt.Tooltip("volume:Q", title="Volume", format=",.0f"),
                    ],
                )
            )
            st.altair_chart(volume_chart, width="stretch")

        with table_tab:
            st.dataframe(
                display_metrics,
                width="stretch",
                hide_index=True,
                column_config={
                    "minute": st.column_config.DatetimeColumn(
                        "Minute (UTC)",
                        format="YYYY-MM-DD HH:mm",
                    )
                },
            )
        st.caption("Refreshes every 5 seconds")
    except Exception as error:
        st.error(f"Database unavailable: {error}")


show_live_metrics()
