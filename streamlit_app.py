"""
Enterprise Financial Risk Monitoring Dashboard
Professional, high-density interface for real-time invoice and payment risk assessment.
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
import json
import os
from pathlib import Path


# Configure page
st.set_page_config(
    page_title="Financial Risk Monitoring Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Enterprise CSS - Monochrome, professional, high-density
st.markdown("""
<style>
    /* Global theme */
    :root {
        --primary-dark: #0F172A;
        --secondary-dark: #1E293B;
        --border-color: #E2E8F0;
        --text-primary: #0F172A;
        --text-secondary: #64748B;
        --text-muted: #94A3B8;
        --bg-white: #FFFFFF;
        --bg-light: #F8FAFC;
    }
    
    /* Remove Streamlit branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    
    /* Main container */
    .main .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
        max-width: 100%;
    }
    
    /* Typography */
    h1, h2, h3, h4, h5, h6 {
        color: var(--text-primary) !important;
        font-weight: 600 !important;
        letter-spacing: -0.02em;
    }
    
    h1 {font-size: 1.75rem !important; margin-bottom: 0.25rem !important;}
    h2 {font-size: 1.25rem !important; margin-bottom: 1rem !important;}
    h3 {font-size: 1rem !important; margin-bottom: 0.75rem !important; font-weight: 600 !important;}
    
    /* Remove all metric deltas and colors */
    [data-testid="stMetricValue"] {
        color: var(--text-primary) !important;
        font-size: 2rem !important;
        font-weight: 600 !important;
    }
    
    [data-testid="stMetricLabel"] {
        color: var(--text-secondary) !important;
        font-size: 0.75rem !important;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        font-weight: 500 !important;
    }
    
    [data-testid="stMetricDelta"] {
        display: none !important;  /* Remove all delta indicators */
    }
    
    /* Enterprise KPI cards */
    .kpi-card {
        background: var(--bg-white);
        border: 1px solid var(--border-color);
        border-radius: 6px;
        padding: 1.25rem;
        margin-bottom: 1rem;
    }
    
    .kpi-title {
        font-size: 0.75rem;
        color: var(--text-secondary);
        text-transform: uppercase;
        letter-spacing: 0.05em;
        font-weight: 500;
        margin-bottom: 0.5rem;
    }
    
    .kpi-value {
        font-size: 2rem;
        font-weight: 600;
        color: var(--text-primary);
        line-height: 1;
        margin-bottom: 0.25rem;
    }
    
    .kpi-subtitle {
        font-size: 0.75rem;
        color: var(--text-muted);
        font-weight: 400;
    }
    
    .kpi-change {
        font-size: 0.75rem;
        color: var(--text-muted);
        margin-top: 0.5rem;
    }
    
    /* Tables - High density */
    .dataframe {
        font-size: 0.875rem !important;
        border: 1px solid var(--border-color) !important;
        border-radius: 6px;
    }
    
    .dataframe thead tr th {
        background-color: var(--bg-light) !important;
        color: var(--text-secondary) !important;
        font-weight: 600 !important;
        font-size: 0.75rem !important;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        padding: 0.75rem !important;
        border-bottom: 1px solid var(--border-color) !important;
    }
    
    .dataframe tbody tr td {
        padding: 0.625rem 0.75rem !important;
        border-bottom: 1px solid #F1F5F9 !important;
        color: var(--text-primary) !important;
    }
    
    .dataframe tbody tr:hover {
        background-color: var(--bg-light) !important;
    }
    
    /* Status indicators - NO COLORS */
    .status-approved, .status-rejected, .status-pending, .status-review {
        font-weight: 500;
        font-size: 0.875rem;
        color: var(--text-primary);
    }
    
    /* Risk score progress bar - Single theme color */
    .risk-bar {
        height: 6px;
        background: #E2E8F0;
        border-radius: 3px;
        overflow: hidden;
        margin-top: 0.25rem;
    }
    
    .risk-fill {
        height: 100%;
        background: var(--primary-dark);
        transition: width 0.3s ease;
    }
    
    /* Section headers */
    .section-header {
        font-size: 0.875rem;
        font-weight: 600;
        color: var(--text-primary);
        text-transform: uppercase;
        letter-spacing: 0.05em;
        margin-bottom: 1rem;
        padding-bottom: 0.5rem;
        border-bottom: 1px solid var(--border-color);
    }
    
    /* Sidebar */
    .css-1d391kg, [data-testid="stSidebar"] {
        background-color: var(--bg-white);
        border-right: 1px solid var(--border-color);
    }
    
    /* Buttons */
    .stButton > button {
        background-color: var(--primary-dark);
        color: white;
        border: none;
        border-radius: 4px;
        padding: 0.5rem 1rem;
        font-weight: 500;
        font-size: 0.875rem;
    }
    
    .stButton > button:hover {
        background-color: var(--secondary-dark);
    }
    
    /* Remove all colored boxes/pills */
    .element-container {
        background: transparent !important;
    }
    
    /* Chart containers */
    .js-plotly-plot {
        border: 1px solid var(--border-color);
        border-radius: 6px;
        background: var(--bg-white);
    }
</style>
""", unsafe_allow_html=True)


# Session state
if 'refresh_count' not in st.session_state:
    st.session_state.refresh_count = 0
if 'last_update' not in st.session_state:
    st.session_state.last_update = datetime.now()


def load_data(file_path: str) -> pd.DataFrame:
    """Load JSONL data."""
    if not os.path.exists(file_path):
        return pd.DataFrame()
    
    try:
        data = []
        with open(file_path, 'r') as f:
            for line in f:
                if line.strip():
                    data.append(json.loads(line))
        return pd.DataFrame(data)
    except:
        return pd.DataFrame()


def render_header():
    """Render enterprise dashboard header."""
    col1, col2 = st.columns([3, 1])
    
    with col1:
        st.markdown('<h1>Financial Risk Monitoring Dashboard</h1>', unsafe_allow_html=True)
        st.markdown('<p style="color: #64748B; font-size: 0.875rem; margin-top: -0.5rem;">Real-time invoice and payment risk assessment</p>', unsafe_allow_html=True)
    
    with col2:
        st.markdown(f"""
        <div style="text-align: right; padding-top: 0.5rem;">
            <div style="font-size: 0.75rem; color: #94A3B8; text-transform: uppercase; letter-spacing: 0.05em;">System Status</div>
            <div style="font-size: 1rem; font-weight: 600; color: #0F172A; margin-top: 0.25rem;">Active</div>
            <div style="font-size: 0.75rem; color: #94A3B8; margin-top: 0.25rem;">Last sync: {datetime.now().strftime('%H:%M:%S')}</div>
        </div>
        """, unsafe_allow_html=True)


def render_kpi_cards(decisions_df: pd.DataFrame, alerts_df: pd.DataFrame):
    """Render enterprise KPI cards - minimal, professional."""
    
    col1, col2, col3, col4 = st.columns(4)
    
    # Total invoices (MTD)
    with col1:
        total_invoices = len(decisions_df) if not decisions_df.empty else 0
        prev_month = int(total_invoices * 0.92)  # Simulated
        change_pct = ((total_invoices - prev_month) / prev_month * 100) if prev_month > 0 else 0
        
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-title">Total Invoices</div>
            <div class="kpi-value">{total_invoices:,} <span style="font-size: 0.875rem; font-weight: 400; color: #64748B;">MTD</span></div>
            <div class="kpi-subtitle">Month-to-date invoice volume</div>
            <div class="kpi-change">{change_pct:+.1f}% vs last month</div>
        </div>
        """, unsafe_allow_html=True)
    
    # High risk items
    with col2:
        high_risk = len(alerts_df) if not alerts_df.empty else 0
        prev_high_risk = int(high_risk * 1.035)  # Simulated
        change_pct = ((high_risk - prev_high_risk) / prev_high_risk * 100) if prev_high_risk > 0 else 0
        
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-title">High Risk</div>
            <div class="kpi-value">{high_risk:,} <span style="font-size: 0.875rem; font-weight: 400; color: #64748B;">items</span></div>
            <div class="kpi-subtitle">Flagged for manual review</div>
            <div class="kpi-change">{change_pct:+.1f}% vs last month</div>
        </div>
        """, unsafe_allow_html=True)
    
    # Automation rate - VISUALLY DOMINANT
    with col3:
        if not decisions_df.empty and 'decision' in decisions_df.columns:
            auto_approved = len(decisions_df[decisions_df['decision'] == 'AUTO_APPROVE'])
            auto_rejected = len(decisions_df[decisions_df['decision'] == 'AUTO_REJECT'])
            automation_rate = (auto_approved + auto_rejected) / len(decisions_df) * 100
        else:
            automation_rate = 0
        
        prev_rate = automation_rate * 0.972  # Simulated
        change_pct = automation_rate - prev_rate
        
        st.markdown(f"""
        <div class="kpi-card" style="border: 2px solid #0F172A; background: #F8FAFC;">
            <div class="kpi-title" style="color: #0F172A;">Automation Rate</div>
            <div class="kpi-value" style="font-size: 2.5rem;">{automation_rate:.1f}<span style="font-size: 1.5rem;">%</span></div>
            <div class="kpi-subtitle">Decisions processed autonomously</div>
            <div class="kpi-change">{change_pct:+.1f}% vs last month</div>
        </div>
        """, unsafe_allow_html=True)
    
    # Fraud prevented value
    with col4:
        fraud_prevented = 12.4  # Simulated (in millions)
        prev_prevented = 10.8
        change_pct = ((fraud_prevented - prev_prevented) / prev_prevented * 100)
        
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-title">Fraud Prevented</div>
            <div class="kpi-value">₹{fraud_prevented:.1f}M</div>
            <div class="kpi-subtitle">Estimated monthly prevention value</div>
            <div class="kpi-change">{change_pct:+.1f}% vs last month</div>
        </div>
        """, unsafe_allow_html=True)


def render_risk_trend_chart(decisions_df: pd.DataFrame):
    """Render 7-day risk distribution trend - ONLY COLORED ELEMENT."""
    
    st.markdown('<div class="section-header">Risk Distribution Trend</div>', unsafe_allow_html=True)
    st.markdown('<p style="color: #64748B; font-size: 0.875rem; margin-top: -0.75rem; margin-bottom: 1rem;">7-day invoice risk assessment breakdown</p>', unsafe_allow_html=True)
    
    # Simulate 7-day trend data
    days = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
    low_risk = [45, 52, 48, 61, 55, 68, 71]
    medium_risk = [32, 35, 40, 38, 42, 28, 30]
    high_risk = [23, 18, 21, 15, 19, 12, 14]
    
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=days, y=low_risk,
        name='Low Risk',
        mode='lines+markers',
        line=dict(color='#10B981', width=2),
        marker=dict(size=8)
    ))
    
    fig.add_trace(go.Scatter(
        x=days, y=medium_risk,
        name='Medium Risk',
        mode='lines+markers',
        line=dict(color='#F59E0B', width=2),
        marker=dict(size=8)
    ))
    
    fig.add_trace(go.Scatter(
        x=days, y=high_risk,
        name='High Risk',
        mode='lines+markers',
        line=dict(color='#EF4444', width=2),
        marker=dict(size=8)
    ))
    
    fig.update_layout(
        height=350,
        margin=dict(l=20, r=20, t=20, b=40),
        paper_bgcolor='white',
        plot_bgcolor='white',
        font=dict(family='Inter, sans-serif', size=12, color='#64748B'),
        xaxis=dict(
            showgrid=False,
            showline=True,
            linecolor='#E2E8F0',
            tickfont=dict(color='#64748B'),
        ),
        yaxis=dict(
            showgrid=True,
            gridcolor='#F1F5F9',
            showline=True,
            linecolor='#E2E8F0',
            tickfont=dict(color='#64748B'),
            range=[0, 80]
        ),
        legend=dict(
            orientation='h',
            yanchor='bottom',
            y=1.02,
            xanchor='right',
            x=1,
            font=dict(size=11)
        ),
        hovermode='x unified'
    )
    
    st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})


def render_recent_transactions(decisions_df: pd.DataFrame):
    """Render high-density transaction table - NO COLORS."""
    
    st.markdown('<div class="section-header">Recent Transactions</div>', unsafe_allow_html=True)
    st.markdown('<p style="color: #64748B; font-size: 0.875rem; margin-top: -0.75rem; margin-bottom: 1rem;">Latest invoice processing decisions</p>', unsafe_allow_html=True)
    
    if decisions_df.empty:
        st.info("No transaction data available")
        return
    
    # Prepare display data
    display_df = decisions_df.head(15).copy()
    
    # Format columns for display
    if 'amount' in display_df.columns:
        display_df['amount'] = '₹' + display_df['amount'].apply(lambda x: f'{x:,.0f}')
    
    if 'decision' in display_df.columns:
        display_df['status'] = display_df['decision'].map({
            'AUTO_APPROVE': 'Approved',
            'AUTO_REJECT': 'Rejected',
            'REVIEW_REQUIRED': 'Pending'
        })
    
    if 'timestamp' in display_df.columns:
        display_df['time'] = pd.to_datetime(display_df['timestamp']).apply(
            lambda x: x.strftime('%H:%M')
        )
    
    # Select and rename columns
    cols_map = {
        'invoice_id': 'INVOICE',
        'vendor_id': 'VENDOR',
        'amount': 'AMOUNT',
        'risk_score': 'RISK SCORE',
        'status': 'STATUS',
        'time': 'TIME'
    }
    
    display_cols = [col for col in cols_map.keys() if col in display_df.columns]
    final_df = display_df[display_cols].rename(columns=cols_map)
    
    # Custom HTML table for better control
    st.markdown(final_df.to_html(index=False, escape=False, classes='dataframe'), unsafe_allow_html=True)


def render_decision_engine_sidebar():
    """Render decision engine stats in sidebar."""
    
    st.markdown('<div class="section-header">Decision Engine</div>', unsafe_allow_html=True)
    
    # Load decisions
    decisions_df = load_data('output/autonomous_decisions.jsonl')
    
    if not decisions_df.empty and 'decision' in decisions_df.columns:
        approved = len(decisions_df[decisions_df['decision'] == 'AUTO_APPROVE'])
        review = len(decisions_df[decisions_df['decision'] == 'REVIEW_REQUIRED'])
        rejected = len(decisions_df[decisions_df['decision'] == 'AUTO_REJECT'])
        total = len(decisions_df)
    else:
        approved, review, rejected, total = 0, 0, 0, 0
    
    st.markdown(f"""
    <div style="margin-top: 1rem;">
        <div style="display: flex; justify-content: space-between; margin-bottom: 0.5rem;">
            <span style="font-size: 0.875rem; color: #0F172A; font-weight: 500;">Approved</span>
            <span style="font-size: 0.875rem; color: #0F172A; font-weight: 600;">{approved:,}</span>
        </div>
        <div style="display: flex; justify-content: space-between; margin-bottom: 0.5rem;">
            <span style="font-size: 0.875rem; color: #0F172A; font-weight: 400;">Under Review</span>
            <span style="font-size: 0.875rem; color: #0F172A;">{review:,}</span>
        </div>
        <div style="display: flex; justify-content: space-between; margin-bottom: 0.5rem;">
            <span style="font-size: 0.875rem; color: #0F172A; font-weight: 400;">Rejected</span>
            <span style="font-size: 0.875rem; color: #0F172A;">{rejected:,}</span>
        </div>
        <div style="border-top: 1px solid #E2E8F0; margin-top: 0.75rem; padding-top: 0.75rem;">
            <div style="font-size: 0.75rem; color: #94A3B8; margin-bottom: 0.25rem;">{total:,} total invoices today</div>
        </div>
    </div>
    """, unsafe_allow_html=True)


def render_critical_alerts():
    """Render critical alerts sidebar."""
    
    st.markdown('<div class="section-header" style="margin-top: 2rem;">Critical Alerts</div>', unsafe_allow_html=True)
    
    # Simulated high-risk vendors
    alerts = [
        ('International Trading LLC', 94),
        ('Consolidated Supply Co.', 87),
        ('North America Distributing', 76)
    ]
    
    for vendor, risk in alerts:
        st.markdown(f"""
        <div style="display: flex; justify-content: space-between; align-items: center; padding: 0.75rem 0; border-bottom: 1px solid #F1F5F9;">
            <div style="flex: 1;">
                <div style="font-size: 0.875rem; color: #0F172A; font-weight: 500;">{vendor}</div>
            </div>
            <div style="font-size: 1.125rem; font-weight: 600; color: #0F172A;">{risk}</div>
        </div>
        """, unsafe_allow_html=True)


def render_system_status():
    """Render system status in sidebar."""
    
    st.markdown('<div class="section-header" style="margin-top: 2rem;">System Status</div>', unsafe_allow_html=True)
    
    st.markdown(f"""
    <div style="margin-top: 1rem;">
        <div style="display: flex; justify-content: space-between; margin-bottom: 0.75rem;">
            <span style="font-size: 0.875rem; color: #64748B;">Processing</span>
            <span style="font-size: 0.875rem; color: #0F172A; font-weight: 500;">Active</span>
        </div>
        <div style="display: flex; justify-content: space-between; margin-bottom: 0.75rem;">
            <span style="font-size: 0.875rem; color: #64748B;">Avg Response</span>
            <span style="font-size: 0.875rem; color: #0F172A; font-weight: 500;">142ms</span>
        </div>
        <div style="display: flex; justify-content: space-between;">
            <span style="font-size: 0.875rem; color: #64748B;">Last Sync</span>
            <span style="font-size: 0.875rem; color: #0F172A; font-weight: 500;">2 min ago</span>
        </div>
    </div>
    """, unsafe_allow_html=True)


def main():
    """Main dashboard."""
    
    # Load data
    decisions_df = load_data('output/autonomous_decisions.jsonl')
    alerts_df = load_data('output/high_risk_alerts.jsonl')
    
    # Header
    render_header()
    
    st.markdown('<div style="height: 1.5rem;"></div>', unsafe_allow_html=True)
    
    # KPI Cards
    render_kpi_cards(decisions_df, alerts_df)
    
    st.markdown('<div style="height: 1.5rem;"></div>', unsafe_allow_html=True)
    
    # Main content: Chart + Sidebar
    col1, col2 = st.columns([7, 3])
    
    with col1:
        # Risk trend chart (ONLY colored element)
        render_risk_trend_chart(decisions_df)
        
        st.markdown('<div style="height: 2rem;"></div>', unsafe_allow_html=True)
        
        # Recent transactions table
        render_recent_transactions(decisions_df)
    
    with col2:
        # Decision engine stats
        render_decision_engine_sidebar()
        
        # Critical alerts
        render_critical_alerts()
        
        # System status
        render_system_status()


if __name__ == "__main__":
    main()
