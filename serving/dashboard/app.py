"""Telco Churn Prediction — Executive Dashboard."""

import os
from datetime import datetime

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import streamlit as st

API_BASE_URL = os.getenv("API_BASE_URL", "http://churn-api-service:8000")
API_TOKEN = os.getenv("API_TOKEN", "")
SCORES_CSV_PATH = os.getenv("SCORES_CSV_PATH", "/data/churn_scores.csv")
HORIZONS = [30, 60, 90]
TIER_COLORS = {"RED": "#DC3545", "AMBER": "#FFC107", "YELLOW": "#FD7E14", "GREEN": "#28A745"}
TIER_ORDER = ["RED", "AMBER", "YELLOW", "GREEN"]

st.set_page_config(
    page_title="Telco Churn — Risk Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)


def _api_headers() -> dict:
    """Return auth headers when API_TOKEN is configured."""
    if API_TOKEN:
        return {"Authorization": f"Bearer {API_TOKEN}"}
    return {}


@st.cache_data(ttl=300)
def load_scores():
    """Load batch scores from CSV (exported from Delta Lake)."""
    if os.path.exists(SCORES_CSV_PATH):
        df = pd.read_csv(SCORES_CSV_PATH)
        return df
    return pd.DataFrame()


@st.cache_data(ttl=60)
def get_api_health():
    """Check API health status."""
    try:
        resp = requests.get(f"{API_BASE_URL}/ready", headers=_api_headers(), timeout=5)
        return resp.json() if resp.status_code == 200 else {"status": "unavailable"}
    except Exception:
        return {"status": "unreachable"}


@st.cache_data(ttl=60)
def get_model_info():
    """Fetch model metadata from API."""
    try:
        resp = requests.get(f"{API_BASE_URL}/model/info", headers=_api_headers(), timeout=5)
        return resp.json() if resp.status_code == 200 else {}
    except Exception:
        return {}


def score_customer_via_api(features: dict) -> dict:
    """Score a single customer via the REST API."""
    try:
        resp = requests.post(f"{API_BASE_URL}/predict", json=features, headers=_api_headers(), timeout=10)
        if resp.status_code == 200:
            return resp.json()
        return {"error": resp.text}
    except Exception as e:
        return {"error": str(e)}


st.title("UK Telecoms — Customer Churn Risk Dashboard")
st.caption(f"Last refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

with st.sidebar:
    st.header("Controls")

    api_status = get_api_health()
    status_color = "green" if api_status.get("status") == "ready" else "red"
    st.markdown(f"**API Status:** :{status_color}[{api_status.get('status', 'unknown')}]")

    model_info = get_model_info()
    if model_info:
        st.markdown(f"**Models Loaded:** {model_info.get('model_count', 'N/A')}")
        st.markdown(f"**Horizons:** {model_info.get('horizons', [])}")

    st.divider()
    page = st.radio(
        "Navigate",
        ["Executive Summary", "Risk Drilldown", "Live Scoring", "Model Performance"],
    )

df = load_scores()

if page == "Executive Summary":
    if df.empty:
        st.warning("No batch scores available. Upload churn_scores.csv to /data/ or run the batch scoring pipeline.")
        st.stop()

    col1, col2, col3, col4, col5 = st.columns(5)

    total_customers = len(df)
    red_count = len(df[df["risk_tier"] == "RED"])
    amber_count = len(df[df["risk_tier"] == "AMBER"])
    yellow_count = len(df[df["risk_tier"] == "YELLOW"])
    green_count = len(df[df["risk_tier"] == "GREEN"])

    col1.metric("Total Customers", f"{total_customers:,}")
    col2.metric("RED (Imminent)", f"{red_count:,}", delta=f"{red_count/total_customers*100:.1f}%", delta_color="inverse")
    col3.metric("AMBER (Drifting)", f"{amber_count:,}", delta=f"{amber_count/total_customers*100:.1f}%", delta_color="inverse")
    col4.metric("YELLOW (Early)", f"{yellow_count:,}", delta=f"{yellow_count/total_customers*100:.1f}%", delta_color="off")
    col5.metric("GREEN (Safe)", f"{green_count:,}", delta=f"{green_count/total_customers*100:.1f}%")

    st.divider()

    chart_col1, chart_col2 = st.columns(2)

    with chart_col1:
        st.subheader("Risk Tier Distribution")
        tier_counts = df["risk_tier"].value_counts().reindex(TIER_ORDER, fill_value=0)
        fig_donut = go.Figure(data=[go.Pie(
            labels=tier_counts.index,
            values=tier_counts.values,
            hole=0.5,
            marker_colors=[TIER_COLORS[t] for t in tier_counts.index],
            textinfo="label+percent+value",
        )])
        fig_donut.update_layout(height=400, showlegend=True)
        st.plotly_chart(fig_donut, use_container_width=True)

    with chart_col2:
        st.subheader("Churn Probability Distributions by Horizon")
        prob_data = []
        for h in HORIZONS:
            col_name = f"churn_prob_{h}d"
            if col_name in df.columns:
                for val in df[col_name].dropna():
                    prob_data.append({"Horizon": f"{h}-day", "Probability": val})
        if prob_data:
            prob_df = pd.DataFrame(prob_data)
            fig_hist = px.histogram(
                prob_df, x="Probability", color="Horizon",
                nbins=50, barmode="overlay", opacity=0.7,
                color_discrete_sequence=["#DC3545", "#FFC107", "#17A2B8"],
            )
            fig_hist.update_layout(height=400)
            st.plotly_chart(fig_hist, use_container_width=True)

    st.divider()

    st.subheader("Action Queue Summary")
    action_col1, action_col2, action_col3 = st.columns(3)

    with action_col1:
        st.markdown("#### Loyalty Team (RED)")
        st.markdown(f"**{red_count}** customers require emergency outbound calls")
        st.markdown("**SLA:** Within 24 hours")
        if red_count > 0:
            avg_prob = df[df["risk_tier"] == "RED"]["churn_prob_30d"].mean()
            st.markdown(f"**Avg 30d churn probability:** {avg_prob:.1%}")

    with action_col2:
        st.markdown("#### Marketing (AMBER)")
        st.markdown(f"**{amber_count}** customers for digital retention campaigns")
        st.markdown("**SLA:** Within 7 days")
        if amber_count > 0:
            avg_prob = df[df["risk_tier"] == "AMBER"]["churn_prob_60d"].mean()
            st.markdown(f"**Avg 60d churn probability:** {avg_prob:.1%}")

    with action_col3:
        st.markdown("#### Customer Success (YELLOW)")
        st.markdown(f"**{yellow_count}** customers for proactive check-ins")
        st.markdown("**SLA:** Within 30 days")
        if yellow_count > 0:
            avg_prob = df[df["risk_tier"] == "YELLOW"]["churn_prob_90d"].mean()
            st.markdown(f"**Avg 90d churn probability:** {avg_prob:.1%}")

    st.divider()

    st.subheader("Cross-Horizon Risk Heatmap (Avg Probability by Tier)")
    heatmap_data = []
    for tier in TIER_ORDER:
        tier_df = df[df["risk_tier"] == tier]
        if len(tier_df) > 0:
            row = {"Tier": tier}
            for h in HORIZONS:
                col_name = f"churn_prob_{h}d"
                if col_name in tier_df.columns:
                    row[f"{h}d"] = tier_df[col_name].mean()
            heatmap_data.append(row)
    if heatmap_data:
        heatmap_df = pd.DataFrame(heatmap_data).set_index("Tier")
        fig_heat = px.imshow(
            heatmap_df, text_auto=".2%", aspect="auto",
            color_continuous_scale="RdYlGn_r",
            labels=dict(x="Prediction Horizon", y="Risk Tier", color="Avg Probability"),
        )
        fig_heat.update_layout(height=300)
        st.plotly_chart(fig_heat, use_container_width=True)


elif page == "Risk Drilldown":
    if df.empty:
        st.warning("No batch scores available.")
        st.stop()

    st.subheader("Customer-Level Risk Drilldown")

    filter_col1, filter_col2 = st.columns(2)
    with filter_col1:
        selected_tiers = st.multiselect("Risk Tier", TIER_ORDER, default=["RED", "AMBER"])
    with filter_col2:
        prob_threshold = st.slider("Min 30d Churn Probability", 0.0, 1.0, 0.3, 0.05)

    filtered = df[
        (df["risk_tier"].isin(selected_tiers))
        & (df.get("churn_prob_30d", pd.Series(dtype=float)) >= prob_threshold)
    ]

    st.markdown(f"**Showing {len(filtered):,} of {len(df):,} customers**")

    display_cols = ["unique_customer_identifier", "risk_tier"]
    for h in HORIZONS:
        col_name = f"churn_prob_{h}d"
        if col_name in filtered.columns:
            display_cols.append(col_name)
    if "score_date" in filtered.columns:
        display_cols.append("score_date")

    available_cols = [c for c in display_cols if c in filtered.columns]
    st.dataframe(
        filtered[available_cols].sort_values(
            by="churn_prob_30d" if "churn_prob_30d" in available_cols else available_cols[0],
            ascending=False,
        ),
        use_container_width=True,
        height=600,
    )

    csv = filtered[available_cols].to_csv(index=False)
    st.download_button("Download Filtered List (CSV)", csv, "churn_risk_list.csv", "text/csv")


elif page == "Live Scoring":
    st.subheader("Real-Time Customer Scoring")
    st.markdown("Enter customer features to get an instant churn prediction via the API.")

    with st.form("scoring_form"):
        form_col1, form_col2, form_col3 = st.columns(3)

        with form_col1:
            st.markdown("**Customer Info**")
            customer_id = st.text_input("Customer ID", "CUST-001")
            contract_status_ord = st.number_input("Contract Status (1-6)", 1, 6, 3)
            ooc_days = st.number_input("Out-of-Contract Days", -9999, 3650, 0)
            tenure_days = st.number_input("Tenure (days)", 0, 7300, 365)
            technology = st.selectbox("Technology", ["FTTP", "FTTC", "MPF", "WLR", "Unknown"])
            sales_channel = st.selectbox("Sales Channel", ["Online", "Telesales", "Retail", "Partner", "Unknown"])

        with form_col2:
            st.markdown("**Call History**")
            calls_30d = st.number_input("Calls (30d)", 0, 100, 0)
            calls_90d = st.number_input("Calls (90d)", 0, 300, 0)
            loyalty_calls_90d = st.number_input("Loyalty Calls (90d)", 0, 50, 0)
            complaint_calls_90d = st.number_input("Complaint Calls (90d)", 0, 50, 0)
            tech_calls_90d = st.number_input("Tech Calls (90d)", 0, 50, 0)
            dd_cancel_60_day = st.number_input("DD Cancels (60d)", 0, 10, 0)

        with form_col3:
            st.markdown("**Usage & Speed**")
            speed = st.number_input("Advertised Speed (Mbps)", 0.0, 1000.0, 80.0)
            line_speed = st.number_input("Actual Speed (Mbps)", 0.0, 1000.0, 65.0)
            monthly_total_mb = st.number_input("Monthly Usage (MB)", 0.0, 1000000.0, 50000.0)
            usage_mom_change = st.number_input("Usage MoM Change", -1.0, 10.0, 0.0, 0.01)
            prior_cease_count = st.number_input("Prior Cease Count", 0, 20, 0)

        submitted = st.form_submit_button("Score Customer", type="primary")

    if submitted:
        speed_gap_pct = round((speed - line_speed) / speed * 100, 2) if speed > 0 else 0
        tenure_bucket = (
            "0-90d" if tenure_days < 90
            else "90d-1y" if tenure_days < 365
            else "1y-2y" if tenure_days < 730
            else "2y-3y" if tenure_days < 1095
            else "3y+"
        )

        features = {
            "customer_id": customer_id,
            "contract_status_ord": contract_status_ord,
            "contract_dd_cancels": 0,
            "dd_cancel_60_day": dd_cancel_60_day,
            "ooc_days": ooc_days,
            "speed": speed,
            "line_speed": line_speed,
            "speed_gap_pct": speed_gap_pct,
            "tenure_days": tenure_days,
            "calls_30d": calls_30d,
            "calls_90d": calls_90d,
            "calls_180d": 0,
            "loyalty_calls_90d": loyalty_calls_90d,
            "complaint_calls_90d": complaint_calls_90d,
            "tech_calls_90d": tech_calls_90d,
            "avg_talk_time": 0,
            "avg_hold_time": 0,
            "max_hold_time": 0,
            "days_since_last_call": 0,
            "monthly_download_mb": monthly_total_mb * 0.85,
            "monthly_upload_mb": monthly_total_mb * 0.15,
            "monthly_total_mb": monthly_total_mb,
            "avg_daily_download_mb": monthly_total_mb * 0.85 / 30,
            "std_daily_total_mb": 0,
            "active_days_in_month": 30,
            "peak_daily_total_mb": 0,
            "usage_mom_change": usage_mom_change,
            "usage_vs_3mo_avg": 0,
            "prior_cease_count": prior_cease_count,
            "days_since_last_cease": 0,
            "technology": technology,
            "sales_channel": sales_channel,
            "tenure_bucket": tenure_bucket,
        }

        with st.spinner("Scoring..."):
            result = score_customer_via_api(features)

        if "error" in result:
            st.error(f"API Error: {result['error']}")
        else:
            tier = result["risk_tier"]
            color = TIER_COLORS.get(tier, "#6C757D")

            st.markdown(f"### Risk Tier: <span style='color:{color}; font-size:2em;'>{tier}</span>", unsafe_allow_html=True)
            st.markdown(f"**Action:** {result['recommended_action']}")
            st.markdown(f"**Owner:** {result['action_owner']} | **SLA:** {result['action_sla']}")

            st.divider()

            res_cols = st.columns(3)
            for i, pred in enumerate(result.get("predictions", [])):
                with res_cols[i]:
                    prob = pred["churn_probability"]
                    st.metric(
                        f"{pred['horizon_days']}-Day Churn",
                        f"{prob:.1%}",
                        delta=f"Decile {pred['risk_decile']}",
                        delta_color="inverse" if prob > 0.5 else "off",
                    )

            fig_gauge = go.Figure()
            for pred in result.get("predictions", []):
                fig_gauge.add_trace(go.Indicator(
                    mode="gauge+number",
                    value=pred["churn_probability"] * 100,
                    title={"text": f"{pred['horizon_days']}d"},
                    gauge={
                        "axis": {"range": [0, 100]},
                        "bar": {"color": "darkblue"},
                        "steps": [
                            {"range": [0, 30], "color": "#28A745"},
                            {"range": [30, 50], "color": "#FFC107"},
                            {"range": [50, 70], "color": "#FD7E14"},
                            {"range": [70, 100], "color": "#DC3545"},
                        ],
                    },
                    domain={"row": 0, "column": result["predictions"].index(pred)},
                ))
            fig_gauge.update_layout(
                grid={"rows": 1, "columns": 3, "pattern": "independent"},
                height=250,
            )
            st.plotly_chart(fig_gauge, use_container_width=True)


elif page == "Model Performance":
    st.subheader("Model Performance Monitoring")

    if df.empty:
        st.warning("No batch scores available for performance analysis.")
        st.stop()

    st.markdown("#### Prediction Distribution Over Time")
    if "score_date" in df.columns:
        df["score_date"] = pd.to_datetime(df["score_date"])
        for h in HORIZONS:
            col_name = f"churn_prob_{h}d"
            if col_name in df.columns:
                agg = df.groupby("score_date")[col_name].agg(["mean", "std", "median"]).reset_index()
                fig_ts = px.line(
                    agg, x="score_date", y=["mean", "median"],
                    title=f"{h}-Day Churn Probability Trend",
                    labels={"value": "Probability", "score_date": "Score Date"},
                )
                st.plotly_chart(fig_ts, use_container_width=True)

    st.markdown("#### Risk Tier Distribution")
    tier_counts = df["risk_tier"].value_counts().reindex(TIER_ORDER, fill_value=0)
    fig_bar = px.bar(
        x=tier_counts.index, y=tier_counts.values,
        color=tier_counts.index,
        color_discrete_map=TIER_COLORS,
        labels={"x": "Risk Tier", "y": "Customer Count"},
    )
    fig_bar.update_layout(showlegend=False, height=350)
    st.plotly_chart(fig_bar, use_container_width=True)

    st.markdown("#### Churn Probability by Decile")
    for h in HORIZONS:
        decile_col = f"risk_decile_{h}d"
        prob_col = f"churn_prob_{h}d"
        if decile_col in df.columns and prob_col in df.columns:
            decile_agg = df.groupby(decile_col)[prob_col].agg(["mean", "count"]).reset_index()
            decile_agg.columns = ["Decile", "Avg Probability", "Count"]
            fig_dec = px.bar(
                decile_agg, x="Decile", y="Avg Probability",
                text="Count", title=f"{h}-Day Model — Avg Probability by Decile",
                color="Avg Probability", color_continuous_scale="RdYlGn_r",
            )
            fig_dec.update_layout(height=300)
            st.plotly_chart(fig_dec, use_container_width=True)
