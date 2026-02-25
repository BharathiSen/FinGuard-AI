"""
Minimal real-time dashboard for fraud detection monitoring using Streamlit.
Displays alerts, statistics, and vendor insights in real-time.
"""

import streamlit as st
import pathway as pw
import pandas as pd
from datetime import datetime
import time
import json
import os
from pathlib import Path


# Configure Streamlit page
st.set_page_config(
    page_title="FinGuard AI - Fraud Detection",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize session state for tracking
if 'last_alert_count' not in st.session_state:
    st.session_state.last_alert_count = 0
if 'refresh_count' not in st.session_state:
    st.session_state.refresh_count = 0
if 'last_refresh_time' not in st.session_state:
    st.session_state.last_refresh_time = datetime.now()


def load_output_data(file_path: str, format: str = "jsonlines") -> pd.DataFrame:
    """
    Load output data from pipeline outputs.
    
    Args:
        file_path: Path to output file
        format: File format ("jsonlines" or "csv")
    
    Returns:
        DataFrame with data
    """
    
    if not os.path.exists(file_path):
        return pd.DataFrame()
    
    try:
        if format == "jsonlines":
            data = []
            with open(file_path, 'r') as f:
                for line in f:
                    if line.strip():
                        data.append(json.loads(line))
            return pd.DataFrame(data)
        else:
            return pd.read_csv(file_path)
    except Exception as e:
        st.error(f"Error loading {file_path}: {e}")
        return pd.DataFrame()


def display_header():
    """Display dashboard header."""
    
    st.title("🛡️ FinGuard AI - Real-Time Fraud Detection")
    st.markdown("---")
    
    # Status indicator
    col1, col2, col3, col4 = st.columns([2, 1, 1, 1])
    
    with col1:
        st.markdown("### 📊 Live Monitoring Dashboard")
    
    with col2:
        st.metric("Status", "🟢 Active", delta="Streaming")
    
    with col3:
        current_time = datetime.now()
        st.metric("Last Update", current_time.strftime("%H:%M:%S"))
    
    with col4:
        st.metric("Refreshes", st.session_state.refresh_count, delta="Auto-updating")


def display_summary_metrics(alerts_df: pd.DataFrame, vendor_stats_df: pd.DataFrame):
    """Display summary metrics."""
    
    st.markdown("### 📈 Summary Metrics")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_alerts = len(alerts_df)
        new_alerts = max(0, total_alerts - st.session_state.last_alert_count)
        st.metric(
            label="Total Alerts",
            value=total_alerts,
            delta=f"+{new_alerts} new" if new_alerts > 0 else "No change"
        )
        # Update session state
        st.session_state.last_alert_count = total_alerts
    
    with col2:
        high_risk = len(alerts_df[alerts_df.get('risk_level', '') == 'HIGH']) if not alerts_df.empty else 0
        st.metric(
            label="High Risk",
            value=high_risk,
            delta="Critical" if high_risk > 0 else "None",
            delta_color="inverse"
        )
    
    with col3:
        avg_risk = alerts_df['risk_score'].mean() if not alerts_df.empty and 'risk_score' in alerts_df else 0
        st.metric(
            label="Avg Risk Score",
            value=f"{avg_risk:.2f}",
            delta=f"{(avg_risk - 0.5):.2f}" if avg_risk > 0 else "0"
        )
    
    with col4:
        total_vendors = len(vendor_stats_df) if not vendor_stats_df.empty else 0
        st.metric(
            label="Monitored Vendors",
            value=total_vendors
        )


def display_risk_distribution(alerts_df: pd.DataFrame):
    """Display risk level distribution."""
    
    if alerts_df.empty:
        st.info("No alerts to display yet. Waiting for data...")
        return
    
    st.markdown("### 🎯 Risk Distribution")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Risk level breakdown
        if 'risk_level' in alerts_df.columns:
            risk_counts = alerts_df['risk_level'].value_counts()
            st.bar_chart(risk_counts)
        else:
            st.info("Risk level data not available")
    
    with col2:
        # Risk score histogram
        if 'risk_score' in alerts_df.columns:
            st.markdown("**Risk Score Distribution**")
            hist_data = pd.DataFrame({
                'risk_score': alerts_df['risk_score']
            })
            st.line_chart(hist_data)
        else:
            st.info("Risk score data not available")


def display_recent_alerts(alerts_df: pd.DataFrame, limit: int = 10):
    """Display recent high-risk alerts."""
    
    st.markdown("### 🚨 Recent High-Risk Alerts")
    
    if alerts_df.empty:
        st.info("⏳ No alerts yet. System is processing...")
        return
    
    # Sort by risk score and get top N
    recent = alerts_df.nlargest(limit, 'risk_score') if 'risk_score' in alerts_df.columns else alerts_df.head(limit)
    
    # Highlight new alerts added in last refresh
    if len(alerts_df) > st.session_state.last_alert_count and st.session_state.last_alert_count > 0:
        st.success(f"🆕 {len(alerts_df) - st.session_state.last_alert_count} new alerts detected!")
    
    # Display as table
    display_cols = [
        col for col in ['invoice_id', 'vendor_id', 'amount', 'risk_score', 'risk_level', 'timestamp']
        if col in recent.columns
    ]
    
    if display_cols:
        st.dataframe(
            recent[display_cols],
            use_container_width=True,
            hide_index=True
        )
    else:
        st.dataframe(recent, use_container_width=True, hide_index=True)


def display_vendor_insights(vendor_stats_df: pd.DataFrame):
    """Display vendor behavior insights."""
    
    st.markdown("### 👥 Vendor Insights")
    
    if vendor_stats_df.empty:
        st.info("No vendor data available yet")
        return
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**Top Vendors by Activity**")
        if 'total_invoices' in vendor_stats_df.columns:
            top_vendors = vendor_stats_df.nlargest(5, 'total_invoices')[
                ['vendor_id', 'total_invoices']
            ] if 'vendor_id' in vendor_stats_df.columns else vendor_stats_df
            st.dataframe(top_vendors, use_container_width=True, hide_index=True)
        else:
            st.info("Invoice count data not available")
    
    with col2:
        st.markdown("**Highest Average Amounts**")
        if 'avg_amount' in vendor_stats_df.columns:
            high_avg = vendor_stats_df.nlargest(5, 'avg_amount')[
                ['vendor_id', 'avg_amount']
            ] if 'vendor_id' in vendor_stats_df.columns else vendor_stats_df
            st.dataframe(high_avg, use_container_width=True, hide_index=True)
        else:
            st.info("Amount data not available")


def display_explanations(explanations_df: pd.DataFrame, limit: int = 5):
    """Display fraud explanations."""
    
    st.markdown("### 💡 Fraud Explanations")
    
    if explanations_df.empty:
        st.info("No explanations available yet")
        return
    
    # Display top N explanations
    for idx, row in explanations_df.head(limit).iterrows():
        with st.expander(
            f"🔍 {row.get('invoice_id', 'Unknown')} - {row.get('risk_level', 'Unknown')} Risk"
        ):
            st.markdown(f"**Vendor:** {row.get('vendor_id', 'N/A')}")
            st.markdown(f"**Risk Score:** {row.get('risk_score', 'N/A')}")
            
            if 'explanation' in row:
                st.markdown(f"**Explanation:**\n\n{row['explanation']}")
            
            if 'key_factors' in row:
                st.markdown(f"**Key Factors:** {row['key_factors']}")
            
            if 'recommendations' in row:
                st.markdown(f"**Recommendations:**\n\n{row['recommendations']}")


def display_autonomous_decisions(decisions_df: pd.DataFrame):
    """
    🔥 Display Autonomous Decision Engine results.
    Shows real-time automated approve/review/reject decisions.
    """
    
    st.markdown("### 🔥 Autonomous Decision Engine")
    st.markdown("**AI-Powered Automated Invoice Processing**")
    
    if decisions_df.empty:
        st.info("⏳ Waiting for decisions...")
        return
    
    # Decision breakdown metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        auto_approved = len(decisions_df[decisions_df.get('decision', '') == 'AUTO_APPROVE'])
        st.metric(
            label="✅ Auto Approved",
            value=auto_approved,
            delta="Ready for payment",
            delta_color="normal"
        )
    
    with col2:
        review_required = len(decisions_df[decisions_df.get('decision', '') == 'REVIEW_REQUIRED'])
        st.metric(
            label="⚠️ Review Queue",
            value=review_required,
            delta="Manual check needed",
            delta_color="off"
        )
    
    with col3:
        auto_rejected = len(decisions_df[decisions_df.get('decision', '') == 'AUTO_REJECT'])
        st.metric(
            label="🚫 Auto Rejected",
            value=auto_rejected,
            delta="Payment blocked",
            delta_color="inverse"
        )
    
    with col4:
        automation_rate = (auto_approved + auto_rejected) / len(decisions_df) * 100 if len(decisions_df) > 0 else 0
        st.metric(
            label="🤖 Automation Rate",
            value=f"{automation_rate:.1f}%",
            delta="Efficiency boost"
        )
    
    # Decision distribution chart
    st.markdown("#### Decision Distribution")
    
    if 'decision' in decisions_df.columns:
        decision_counts = decisions_df['decision'].value_counts()
        
        # Display as columns with color coding
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("**✅ AUTO_APPROVE**")
            st.success(f"{decision_counts.get('AUTO_APPROVE', 0)} invoices")
            st.caption("Low risk < 30% → Fast-track payment")
        
        with col2:
            st.markdown("**⚠️ REVIEW_REQUIRED**")
            st.warning(f"{decision_counts.get('REVIEW_REQUIRED', 0)} invoices")
            st.caption("Medium risk 30-70% → Manual review")
        
        with col3:
            st.markdown("**🚫 AUTO_REJECT**")
            st.error(f"{decision_counts.get('AUTO_REJECT', 0)} invoices")
            st.caption("High risk > 70% → Block payment")
    
    # Recent decisions table
    st.markdown("#### 📋 Recent Decisions")
    
    display_cols = [
        col for col in ['invoice_id', 'vendor_id', 'amount', 'risk_score', 'decision', 'decision_confidence', 'decision_reason']
        if col in decisions_df.columns
    ]
    
    if display_cols:
        recent_decisions = decisions_df.head(15)[display_cols]
        
        # Apply color coding to decision column
        def highlight_decision(row):
            if row.get('decision') == 'AUTO_APPROVE':
                return ['background-color: #d4edda'] * len(row)
            elif row.get('decision') == 'AUTO_REJECT':
                return ['background-color: #f8d7da'] * len(row)
            else:
                return ['background-color: #fff3cd'] * len(row)
        
        st.dataframe(
            recent_decisions,
            use_container_width=True,
            hide_index=True
        )
    else:
        st.dataframe(decisions_df.head(15), use_container_width=True, hide_index=True)


def main():
    """Main dashboard application."""
    
    # Display header
    display_header()
    
    # Sidebar configuration
    st.sidebar.title("⚙️ Configuration")
    
    output_dir = st.sidebar.text_input(
        "Output Directory",
        value="output",
        help="Directory where pipeline writes output files"
    )
    
    auto_refresh = st.sidebar.checkbox(
        "Auto-refresh",
        value=True,
        help="Automatically refresh data"
    )
    
    refresh_interval = st.sidebar.slider(
        "Refresh Interval (seconds)",
        min_value=1,
        max_value=30,
        value=5
    )
    
    # Manual refresh button
    if st.sidebar.button("🔄 Refresh Now"):
        st.rerun()
    
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 📋 Pipeline Status")
    
    # Check if pipeline is running
    alerts_file = os.path.join(output_dir, "high_risk_alerts.jsonl")
    vendor_file = os.path.join(output_dir, "vendor_stats.jsonl")
    explanations_file = os.path.join(output_dir, "explanations.jsonl")
    decisions_file = os.path.join(output_dir, "autonomous_decisions.jsonl")  # 🔥 Decisions
    
    if os.path.exists(alerts_file):
        st.sidebar.success("✅ Pipeline Active")
        # Show file modification time
        mod_time = datetime.fromtimestamp(os.path.getmtime(alerts_file))
        st.sidebar.caption(f"Last updated: {mod_time.strftime('%H:%M:%S')}")
    else:
        st.sidebar.warning("⚠️ Waiting for pipeline data...")
        st.sidebar.info(
            "Start the pipeline with:\n```bash\npython pipeline.py\n```"
        )
    
    st.sidebar.markdown("---")
    st.sidebar.markdown("""
    ### 📖 Quick Guide
    
    1. **Start Pipeline**: Run `python pipeline.py`
    2. **Monitor**: View real-time alerts here
    3. **🔥 Auto Decisions**: AI approves/reviews/rejects
    4. **Investigate**: Click expanders for details
    5. **Export**: Download data using buttons
    
    **Decision Logic:**
    - Risk < 30% → ✅ Auto Approve
    - Risk 30-70% → ⚠️ Review Required
    - Risk > 70% → 🚫 Auto Reject
    """)
    
    # Load data
    alerts_df = load_output_data(alerts_file, format="jsonlines")
    vendor_stats_df = load_output_data(vendor_file, format="jsonlines")
    explanations_df = load_output_data(explanations_file, format="jsonlines")
    decisions_df = load_output_data(decisions_file, format="jsonlines")  # 🔥 Decisions
    
    # Display dashboard sections
    display_summary_metrics(alerts_df, vendor_stats_df)
    
    st.markdown("---")
    
    # 🔥 Autonomous Decision Engine - PROMINENT DISPLAY
    display_autonomous_decisions(decisions_df)
    
    st.markdown("---")
    
    # Two-column layout for charts and tables
    col1, col2 = st.columns([1, 1])
    
    with col1:
        display_risk_distribution(alerts_df)
    
    with col2:
        display_vendor_insights(vendor_stats_df)
    
    st.markdown("---")
    
    # Recent alerts
    display_recent_alerts(alerts_df, limit=10)
    
    st.markdown("---")
    
    # Explanations
    display_explanations(explanations_df, limit=5)
    
    # Export options
    st.markdown("---")
    st.markdown("### 📥 Export Data")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if not alerts_df.empty:
            csv = alerts_df.to_csv(index=False)
            st.download_button(
                label="Download Alerts (CSV)",
                data=csv,
                file_name=f"alerts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
    
    with col2:
        if not vendor_stats_df.empty:
            csv = vendor_stats_df.to_csv(index=False)
            st.download_button(
                label="Download Vendor Stats (CSV)",
                data=csv,
                file_name=f"vendor_stats_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
    
    with col3:
        if not explanations_df.empty:
            csv = explanations_df.to_csv(index=False)
            st.download_button(
                label="Download Explanations (CSV)",
                data=csv,
                file_name=f"explanations_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
    
    # Auto-refresh with improved UX
    if auto_refresh:
        # Update refresh tracking
        st.session_state.refresh_count += 1
        st.session_state.last_refresh_time = datetime.now()
        
        # Display countdown in sidebar
        with st.sidebar:
            st.markdown("---")
            st.markdown(f"🔄 Auto-refresh in {refresh_interval}s...")
            progress_bar = st.progress(0)
        
        # Sleep and trigger rerun
        time.sleep(refresh_interval)
        st.rerun()


if __name__ == "__main__":
    main()
