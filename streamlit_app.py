import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import os

# ============================================
# PAGE CONFIGURATION
# ============================================

st.set_page_config(
    page_title="100 Days to Hireable Tracker",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================
# DATABASE CONNECTION
# ============================================

def get_database_connection():
    """
    Create PostgreSQL connection using Streamlit secrets for cloud deployment
    Falls back to environment variables for local development
    """
    try:
        # Try Streamlit secrets first (for cloud deployment)
        if hasattr(st, 'secrets') and 'database' in st.secrets:
            conn = psycopg2.connect(
                dbname=st.secrets["database"]["dbname"],
                user=st.secrets["database"]["user"],
                password=st.secrets["database"]["password"],
                host=st.secrets["database"]["host"],
                port=st.secrets["database"]["port"]
            )
        # Fall back to environment variables (for local development)
        else:
            conn = psycopg2.connect(
                dbname=os.getenv('DB_NAME', 'harvest_tracker'),
                user=os.getenv('DB_USER', 'fandy'),
                password=os.getenv('DB_PASSWORD', 'password'),
                host=os.getenv('DB_HOST', 'localhost'),
                port=os.getenv('DB_PORT', '5432')
            )
        return conn
    except Exception as e:
        st.error(f"❌ Database connection failed: {e}")
        st.info("💡 Make sure PostgreSQL is running and credentials are correct")
        return None

# ============================================
# DATA LOADING FUNCTIONS
# ============================================

@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_weekly_summary():
    """Load weekly CT:VT summary for 100 Days Challenge"""
    conn = get_database_connection()
    if conn is None:
        return pd.DataFrame()
    
    query = """
    SELECT 
        week_number,
        week_start,
        SUM(CASE WHEN time_type = 'CT' THEN hours ELSE 0 END) as ct_hours,
        SUM(CASE WHEN time_type = 'VT' THEN hours ELSE 0 END) as vt_hours,
        SUM(CASE WHEN time_type IN ('CT', 'VT') THEN hours ELSE 0 END) as total_hours,
        ROUND(
            SUM(CASE WHEN time_type = 'CT' THEN hours ELSE 0 END) / 
            NULLIF(SUM(CASE WHEN time_type IN ('CT', 'VT') THEN hours ELSE 0 END), 0) * 100, 
            1
        ) as ct_percentage,
        ROUND(
            SUM(CASE WHEN time_type = 'VT' THEN hours ELSE 0 END) / 
            NULLIF(SUM(CASE WHEN time_type IN ('CT', 'VT') THEN hours ELSE 0 END), 0) * 100, 
            1
        ) as vt_percentage,
        CONCAT(
            ROUND(SUM(CASE WHEN time_type = 'CT' THEN hours ELSE 0 END) / 
                  NULLIF(SUM(CASE WHEN time_type IN ('CT', 'VT') THEN hours ELSE 0 END), 0) * 100), 
            ':', 
            ROUND(SUM(CASE WHEN time_type = 'VT' THEN hours ELSE 0 END) / 
                  NULLIF(SUM(CASE WHEN time_type IN ('CT', 'VT') THEN hours ELSE 0 END), 0) * 100)
        ) as ct_vt_ratio
    FROM harvest_time_tracking
    WHERE time_type IN ('CT', 'VT')
      AND week_start >= '2025-12-07'  -- Only show challenge weeks
    GROUP BY week_number, week_start
    ORDER BY week_start DESC
    LIMIT 20
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

@st.cache_data(ttl=300)
def load_ct_breakdown():
    """Load coding time breakdown by category and type"""
    conn = get_database_connection()
    if conn is None:
        return pd.DataFrame()
    
    query = """
    SELECT 
        week_start,
        week_number,
        ct_category,
        ct_type,
        SUM(hours) as hours,
        COUNT(*) as entry_count
    FROM harvest_time_tracking
    WHERE time_type = 'CT'
      AND week_start >= '2025-12-07'  -- Only challenge weeks
    GROUP BY week_start, week_number, ct_category, ct_type
    ORDER BY week_start DESC, hours DESC
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

@st.cache_data(ttl=300)
def load_vt_breakdown():
    """Load video time breakdown"""
    conn = get_database_connection()
    if conn is None:
        return pd.DataFrame()
    
    query = """
    SELECT 
        week_start,
        week_number,
        vt_category,
        day_number,
        SUM(hours) as hours,
        COUNT(*) as entry_count
    FROM harvest_time_tracking
    WHERE time_type = 'VT'
      AND week_start >= '2025-12-07'  -- Only challenge weeks
    GROUP BY week_start, week_number, vt_category, day_number
    ORDER BY week_start DESC, day_number
    """
    
@st.cache_data(ttl=300)
def load_100_days_progress():
    """Load 100 Days challenge progress"""
    conn = get_database_connection()
    if conn is None:
        return pd.DataFrame()
    
    query = """
    SELECT 
        day_number,
        MIN(date) as first_date,
        SUM(hours) as total_hours_on_day,
        100 - CAST(day_number AS INTEGER) as days_remaining,
        ROUND(CAST(day_number AS DECIMAL) / 100 * 100, 1) as progress_percentage
    FROM harvest_time_tracking
    WHERE day_number IS NOT NULL
      AND time_type = 'VT'
    GROUP BY day_number
    ORDER BY day_number
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

@st.cache_data(ttl=300)
def load_current_week_stats():
    """Load current week statistics"""
    conn = get_database_connection()
    if conn is None:
        return pd.DataFrame()
    
    query = """
    SELECT 
        date,
        task,
        time_type,
        hours,
        ct_category,
        vt_category,
        day_number
    FROM harvest_time_tracking
    WHERE week_start = (
        SELECT MAX(week_start) 
        FROM harvest_time_tracking 
        WHERE date >= '2025-12-07'
    )
    ORDER BY date DESC, hours DESC
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

@st.cache_data(ttl=300)
def load_dd_vs_shipping():
    """Load Deep Dive vs Shipping breakdown"""
    conn = get_database_connection()
    if conn is None:
        return pd.DataFrame()
    
    query = """
    SELECT 
        week_start,
        week_number,
        SUM(CASE WHEN ct_type = 'Deep_Dive' THEN hours ELSE 0 END) as deep_dive_hours,
        SUM(CASE WHEN ct_type = 'Shipping' THEN hours ELSE 0 END) as shipping_hours,
        SUM(CASE WHEN ct_type IN ('Deep_Dive', 'Shipping') THEN hours ELSE 0 END) as total_categorized,
        CONCAT(
            ROUND(SUM(CASE WHEN ct_type = 'Deep_Dive' THEN hours ELSE 0 END) / 
                  NULLIF(SUM(CASE WHEN ct_type IN ('Deep_Dive', 'Shipping') THEN hours ELSE 0 END), 0) * 100),
            ':',
            ROUND(SUM(CASE WHEN ct_type = 'Shipping' THEN hours ELSE 0 END) / 
                  NULLIF(SUM(CASE WHEN ct_type IN ('Deep_Dive', 'Shipping') THEN hours ELSE 0 END), 0) * 100)
        ) as dd_shipping_ratio
    FROM harvest_time_tracking
    WHERE time_type = 'CT'
      AND date >= '2025-12-31'  -- Only after Deep Dive/Shipping started
      AND ct_type IN ('Deep_Dive', 'Shipping')
    GROUP BY week_start, week_number
    ORDER BY week_start DESC
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

# ============================================
# CUSTOM CSS
# ============================================

st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        font-weight: bold;
        text-align: center;
        padding: 1rem 0;
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #667eea;
    }
    .success-metric {
        border-left-color: #10b981;
    }
    .warning-metric {
        border-left-color: #f59e0b;
    }
    .danger-metric {
        border-left-color: #ef4444;
    }
</style>
""", unsafe_allow_html=True)

# ============================================
# MAIN APP
# ============================================

def main():
    # Header
    st.markdown('<h1 class="main-header">🎯 100 Days to Hireable Tracker</h1>', unsafe_allow_html=True)
    st.markdown("### Data Engineer | Music Producer | Full Stack Developer")

    # Social links
    col1, col2, col3 = st.columns([1, 1, 2])
    with col1:
        st.markdown("[![LinkedIn](https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/jeffandy/)")
    with col2:
        st.markdown("[![YouTube](https://img.shields.io/badge/YouTube-FF0000?style=for-the-badge&logo=youtube&logoColor=white)](https://youtube.com/playlist?list=PLQjGOmN1hO_lLIiEqIUR2SiACnGt3V59G&si=uulsP6Kxux7h0q7F)")
    
    st.markdown("---")
    
    # Embedded YouTube video
    st.markdown("## 🎥 100 Days Journey")
    st.markdown("""
    <iframe width="100%" height="400" 
    src="https://www.youtube.com/embed/I448nR-uGqc?si=GwIl0J8n44d98P-_" 
    title="YouTube video player" frameborder="0" 
    allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" 
    referrerpolicy="strict-origin-when-cross-origin" allowfullscreen></iframe>
    """, unsafe_allow_html=True)
    
    st.markdown("---")

    # Sidebar
    with st.sidebar:
        st.markdown("### 📊 Dashboard Controls")
        
        refresh_button = st.button("🔄 Refresh Data", use_container_width=True)
        if refresh_button:
            st.cache_data.clear()
            st.rerun()
        
        st.markdown("---")
        st.markdown("### ⚙️ Settings")
        show_all_weeks = st.checkbox("Show all weeks", value=False)
        weeks_to_show = st.slider("Weeks to display", 4, 20, 8) if not show_all_weeks else 20
        
        st.markdown("---")
        st.markdown("### 📝 About")
        st.markdown("""
        Tracking my journey to become hireable as a Data Engineer through:
        - **CT (Coding Time):** Python, SQL, Data Engineering
        - **VT (Video Time):** Documenting daily progress
        - **100 Days Challenge:** Building projects and skills
        
        **Sabbath Observance:** Friday evening through Saturday
        """)
    
    # Load data
    weekly_summary = load_weekly_summary()
    ct_breakdown = load_ct_breakdown()
    vt_breakdown = load_vt_breakdown()
    progress_100_days = load_100_days_progress()
    current_week = load_current_week_stats()
    dd_shipping = load_dd_vs_shipping()
    
    # Safety check: convert None to empty DataFrame
    if weekly_summary is None:
        weekly_summary = pd.DataFrame()
    if ct_breakdown is None:
        ct_breakdown = pd.DataFrame()
    if vt_breakdown is None:
        vt_breakdown = pd.DataFrame()
    if progress_100_days is None:
        progress_100_days = pd.DataFrame()
    if current_week is None:
        current_week = pd.DataFrame()
    if dd_shipping is None:
        dd_shipping = pd.DataFrame()
    
    # Check if data loaded successfully
    if weekly_summary.empty:
        st.error("❌ No data found. Please run the ETL pipeline first.")
        st.code("python harvest_etl_pipeline.py", language="bash")
        return
    
    # ============================================
    # CURRENT WEEK OVERVIEW
    # ============================================
    
    st.markdown("## 📅 Current Week Overview")
    
    if not current_week.empty:
        col1, col2, col3, col4 = st.columns(4)
        
        ct_this_week = current_week[current_week['time_type'] == 'CT']['hours'].sum()
        vt_this_week = current_week[current_week['time_type'] == 'VT']['hours'].sum()
        total_this_week = ct_this_week + vt_this_week
        
        if total_this_week > 0:
            ct_pct = (ct_this_week / total_this_week * 100)
            vt_pct = (vt_this_week / total_this_week * 100)
        else:
            ct_pct = vt_pct = 0
        
        with col1:
            st.metric("💻 Coding Time", f"{ct_this_week:.1f} hrs", f"{ct_pct:.0f}%")
        
        with col2:
            st.metric("🎥 Video Time", f"{vt_this_week:.1f} hrs", f"{vt_pct:.0f}%")
        
        with col3:
            st.metric("⏱️ Total Hours", f"{total_this_week:.1f} hrs")
        
        with col4:
            ratio_color = "🟢" if 60 <= ct_pct <= 80 else "🟡" if 50 <= ct_pct <= 90 else "🔴"
            st.metric(f"{ratio_color} CT:VT Ratio", f"{ct_pct:.0f}:{vt_pct:.0f}")
    else:
        st.info("📊 No data recorded for current week yet")
    
    st.markdown("---")
    
    # ============================================
    # KEY METRICS ROW
    # ============================================
    
    st.markdown("## 📈 Key Metrics")
    
    col1, col2, col3, col4 = st.columns(4)
    
    # Get the most recent COMPLETED week (not current partial week)
    # Week 5 (Jan 4-10) is current/incomplete, so show Week 4 (Dec 28-Jan 3)
    if len(weekly_summary) >= 2:
        latest_complete_week = weekly_summary.iloc[1]  # Second row is most recent complete week
    elif not weekly_summary.empty:
        latest_complete_week = weekly_summary.iloc[0]
    else:
        latest_complete_week = None
    
    with col1:
        if latest_complete_week is not None:
            st.metric(
                "Last Week Total",
                f"{latest_complete_week['total_hours']:.1f} hrs",
                help="Total hours logged last completed week"
            )
    
    with col2:
        if latest_complete_week is not None:
            ratio_status = "✅" if 60 <= latest_complete_week['ct_percentage'] <= 80 else "⚠️"
            st.metric(
                f"{ratio_status} Last Week Ratio",
                latest_complete_week['ct_vt_ratio'],
                help="Target: 70:30 CT:VT"
            )
    
    with col3:
        # Calculate actual challenge days (excluding Sabbaths)
        try:
            conn = get_database_connection()
            if conn:
                query = """
                SELECT COUNT(DISTINCT date) as days_completed
                FROM harvest_time_tracking
                WHERE date >= '2025-12-07'
                  AND time_type IN ('CT', 'VT')
                  AND day_of_week NOT IN ('Saturday')
                """
                days_df = pd.read_sql_query(query, conn)
                conn.close()
                current_day = int(days_df['days_completed'].iloc[0])
            else:
                current_day = 10  # Fallback
        except:
            current_day = 10  # Fallback
            
        st.metric(
            "Days Completed",
            f"{current_day}/100",
            f"{100 - current_day} remaining"
        )
    
    with col4:
        if not dd_shipping.empty and not dd_shipping.iloc[0]['dd_shipping_ratio'] == 'nan:nan':
            latest_dd_ship = dd_shipping.iloc[0]['dd_shipping_ratio']
            st.metric(
                "Deep Dive:Shipping",
                latest_dd_ship,
                help="Target: 30:70"
            )
    
    st.markdown("---")
    
    # ============================================
    # WEEKLY CT:VT TREND
    # ============================================
    
    st.markdown("## 📊 Weekly CT:VT Trend")
    
    if not weekly_summary.empty:
        # Limit to selected weeks
        weekly_display = weekly_summary.head(weeks_to_show).sort_values('week_start')
        
        # Create labels with week numbers
        weekly_display['week_label'] = weekly_display.apply(
            lambda row: f"Week {int(row['week_number'])}<br>{row['week_start'].strftime('%b %d')}", 
            axis=1
        )
        
        fig = go.Figure()
        
        fig.add_trace(go.Bar(
            name='Coding Time',
            x=weekly_display['week_label'],
            y=weekly_display['ct_hours'],
            marker_color='#667eea',
            text=weekly_display['ct_hours'].round(1),
            textposition='inside'
        ))
        
        fig.add_trace(go.Bar(
            name='Video Time',
            x=weekly_display['week_label'],
            y=weekly_display['vt_hours'],
            marker_color='#764ba2',
            text=weekly_display['vt_hours'].round(1),
            textposition='inside'
        ))
        
        # Add target line at 70% CT
        fig.add_hline(
            y=weekly_display['total_hours'].mean() * 0.7,
            line_dash="dash",
            line_color="green",
            annotation_text="70% CT Target",
            annotation_position="right"
        )
        
        fig.update_layout(
            barmode='stack',
            title="Weekly Time Distribution (CT: Coding Time, VT: Video Time)",
            xaxis_title="Week",
            yaxis_title="Hours",
            height=400,
            hovermode='x unified'
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Show CT:VT ratio trend
        fig_ratio = go.Figure()
        
        fig_ratio.add_trace(go.Scatter(
            x=weekly_display['week_start'],
            y=weekly_display['ct_percentage'],
            mode='lines+markers',
            name='CT Percentage',
            line=dict(color='#667eea', width=3),
            marker=dict(size=10)
        ))
        
        # Add target zone
        fig_ratio.add_hrect(
            y0=60, y1=80,
            fillcolor="green", opacity=0.1,
            annotation_text="Target Zone (60-80%)",
            annotation_position="left"
        )
        
        fig_ratio.update_layout(
            title="CT Percentage Trend (Target: 70%)",
            xaxis_title="Week Starting",
            yaxis_title="CT Percentage",
            height=300,
            yaxis=dict(range=[0, 100])
        )
        
        st.plotly_chart(fig_ratio, use_container_width=True)
    
    st.markdown("---")
    
    # ============================================
    # CODING TIME BREAKDOWN
    # ============================================
    
    st.markdown("## 💻 Coding Time Breakdown")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### By Subject Area")
        if not ct_breakdown.empty:
            # Aggregate by category
            ct_by_category = ct_breakdown.groupby('ct_category')['hours'].sum().reset_index()
            ct_by_category = ct_by_category.sort_values('hours', ascending=False)
            
            fig_ct = px.pie(
                ct_by_category,
                values='hours',
                names='ct_category',
                title='Time Distribution by Subject',
                color_discrete_sequence=px.colors.sequential.Purples_r
            )
            fig_ct.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig_ct, use_container_width=True)
    
    with col2:
        st.markdown("### Deep Dive vs Shipping")
        if not dd_shipping.empty:
            # Get recent weeks
            dd_ship_display = dd_shipping.head(weeks_to_show).sort_values('week_start')
            
            fig_dd = go.Figure()
            
            fig_dd.add_trace(go.Bar(
                name='Deep Dive',
                x=dd_ship_display['week_start'],
                y=dd_ship_display['deep_dive_hours'],
                marker_color='#3b82f6'
            ))
            
            fig_dd.add_trace(go.Bar(
                name='Shipping',
                x=dd_ship_display['week_start'],
                y=dd_ship_display['shipping_hours'],
                marker_color='#10b981'
            ))
            
            fig_dd.update_layout(
                barmode='stack',
                title='Deep Dive vs Shipping (Target: 30:70)',
                xaxis_title='Week Starting',
                yaxis_title='Hours',
                height=400
            )
            
            st.plotly_chart(fig_dd, use_container_width=True)
    
    st.markdown("---")
    
    # ============================================
    # 100 DAYS PROGRESS
    # ============================================
    
    st.markdown("## 🎯 100 Days to Hireable Progress")
    
    if not progress_100_days.empty:
        col1, col2 = st.columns([2, 1])
        
        with col1:
            fig_progress = go.Figure()
            
            fig_progress.add_trace(go.Scatter(
                x=progress_100_days['day_number'],
                y=progress_100_days['day_number'],
                mode='lines+markers',
                name='Progress',
                line=dict(color='#667eea', width=3),
                marker=dict(size=8),
                fill='tozeroy',
                fillcolor='rgba(102, 126, 234, 0.1)'
            ))
            
            # Add goal line
            fig_progress.add_trace(go.Scatter(
                x=[0, 100],
                y=[0, 100],
                mode='lines',
                name='Goal',
                line=dict(color='green', dash='dash', width=2)
            ))
            
            fig_progress.update_layout(
                title='Days Completed Over Time',
                xaxis_title='Day Number',
                yaxis_title='Progress',
                height=400,
                xaxis=dict(range=[0, 100]),
                yaxis=dict(range=[0, 100])
            )
            
            st.plotly_chart(fig_progress, use_container_width=True)
        
        with col2:
            st.markdown("### 📊 Progress Stats")
            
            # Calculate actual challenge days completed (excluding Sabbaths)
            # Use a new query to ensure it's not cached
            conn = get_database_connection()
            if conn:
                try:
                    query = """
                    SELECT COUNT(DISTINCT date) as days_completed
                    FROM harvest_time_tracking
                    WHERE date >= '2025-12-07'
                      AND time_type IN ('CT', 'VT')
                      AND day_of_week NOT IN ('Saturday')
                    """
                    days_df = pd.read_sql_query(query, conn)
                    current_day = int(days_df['days_completed'].iloc[0]) if not days_df.empty else 0
                    st.write(f"DEBUG: Query returned {current_day} days")  # Debug line
                except Exception as e:
                    st.error(f"Error calculating days: {e}")
                    current_day = 0
                finally:
                    conn.close()
            else:
                current_day = 0
            
            days_remaining = 100 - current_day
            
            # Get total hours from weekly summary
            total_hours_logged = weekly_summary['total_hours'].sum() if not weekly_summary.empty else 0
            avg_hours_per_day = total_hours_logged / current_day if current_day > 0 else 0
            
            st.metric("Current Day", f"{current_day}/100", f"{days_remaining} remaining")
            st.metric("Total Hours Logged", f"{total_hours_logged:.1f}")
            st.metric("Avg Hours/Day", f"{avg_hours_per_day:.1f}")
            
            # Progress bar
            progress_pct = current_day / 100
            st.progress(progress_pct)
            st.caption(f"{progress_pct*100:.1f}% Complete")
    
    st.markdown("---")
    
    # ============================================
    # VIDEO PRODUCTION BREAKDOWN
    # ============================================
    
    st.markdown("## 🎥 Video Production Breakdown")
    
    if not vt_breakdown.empty:
        # Aggregate by category
        vt_by_category = vt_breakdown.groupby('vt_category')['hours'].sum().reset_index()
        vt_by_category = vt_by_category.sort_values('hours', ascending=True)
        
        fig_vt = px.bar(
            vt_by_category,
            x='hours',
            y='vt_category',
            orientation='h',
            title='Total Time by Video Activity',
            color='hours',
            color_continuous_scale='Purples'
        )
        fig_vt.update_layout(height=300)
        
        st.plotly_chart(fig_vt, use_container_width=True)
        
        # Show insight
        if 'Editing' in vt_by_category['vt_category'].values:
            editing_hours = vt_by_category[vt_by_category['vt_category'] == 'Editing']['hours'].values[0]
            filming_hours = vt_by_category[vt_by_category['vt_category'] == 'Filming']['hours'].values[0] if 'Filming' in vt_by_category['vt_category'].values else 0
            
            if filming_hours > 0:
                ratio = editing_hours / filming_hours
                st.info(f"📊 **Insight:** You spend {ratio:.1f}x more time editing than filming. Consider streamlining your editing workflow!")
    
    st.markdown("---")
    
    # ============================================
    # DATA TABLE
    # ============================================
    
    st.markdown("## 📋 Weekly Summary Table")
    
    if not weekly_summary.empty:
        display_cols = ['week_start', 'ct_hours', 'vt_hours', 'total_hours', 'ct_vt_ratio']
        st.dataframe(
            weekly_summary[display_cols].head(weeks_to_show),
            column_config={
                "week_start": "Week Starting",
                "ct_hours": st.column_config.NumberColumn("CT Hours", format="%.1f"),
                "vt_hours": st.column_config.NumberColumn("VT Hours", format="%.1f"),
                "total_hours": st.column_config.NumberColumn("Total Hours", format="%.1f"),
                "ct_vt_ratio": "CT:VT Ratio"
            },
            hide_index=True,
            use_container_width=True
        )
    
    # Footer
    st.markdown("---")
    st.markdown("""
    <div style='text-align: center; color: #666;'>
        <p>Built by KungFu Fandy | Data Engineer on the come up</p>
        <p>🙏 Agree In Prayer (AIP) - Daily 12:15-12:45 PM EST</p>
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()
