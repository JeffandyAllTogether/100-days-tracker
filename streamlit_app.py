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
    .code-editor {
        background-color: #1e1e1e;
        border-radius: 8px;
        padding: 20px;
        font-family: 'Courier New', monospace;
        font-size: 0.9rem;
        color: #d4d4d4;
        max-height: 500px;
        overflow-y: auto;
    }
    .code-header {
        background-color: #2d2d2d;
        padding: 10px;
        border-radius: 8px 8px 0 0;
        color: #cccccc;
        font-family: 'Courier New', monospace;
    }
    .line-number {
        color: #858585;
        display: inline-block;
        width: 50px;
        min-width: 50px;
        text-align: left;
        margin-right: 20px;
        padding-right: 10px;
        border-right: 1px solid #3e3e3e;
        user-select: none;
    }
    .keyword {
        color: #569cd6;
    }
    .string {
        color: #ce9178;
    }
    .function {
        color: #dcdcaa;
    }
    .comment {
        color: #6a9955;

    }
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
    # ============================================
    # HERO IMAGE SECTION (For Featured App Thumbnail)
    # ============================================
    
    col1, col2, col3 = st.columns([1, 3, 1])
    with col2:
        
        if os.path.exists('preview.png'):
            st.image('preview.png', use_container_width=True)
    
    st.markdown("---")

    
    
    # ============================================
    # DASHBOARD HEADER
    # ============================================
    
    # Get current day first (needed for header)
    conn = get_database_connection()
    if conn:
        try:
            query = """
            SELECT COUNT(DISTINCT date) as days_completed
            FROM harvest_time_tracking
            WHERE date >= '2025-12-07'
              AND time_type IN ('CT', 'VT')
              AND day_of_week != 'Saturday'
            """
            days_df = pd.read_sql_query(query, conn)
            current_day = int(days_df['days_completed'].iloc[0]) if not days_df.empty else 0
        except:
            current_day = 28  # Fallback
        finally:
            conn.close()
    else:
        current_day = 28  # Fallback
    
    # Header with dynamic day count
    st.markdown(f'<h1 class="main-header">🎯 DAY {current_day} / 100 Days of Code!</h1>', unsafe_allow_html=True)
    
    # Centered subtitle
    st.markdown('<p style="text-align: center; font-size: 1.3rem; margin-bottom: 0.5rem;">Data Engineer | Music Producer | Full Stack Developer</p>', unsafe_allow_html=True)
    
    # Centered call-to-action
    st.markdown('<p style="text-align: center; font-size: 1rem; color: #888; margin-bottom: 1rem;">Leave Jeffandy an encouraging comment on LinkedIn or YouTube! 💬</p>', unsafe_allow_html=True)

# Centered social links
    st.markdown("""
    <div style="display: flex; justify-content: center; align-items: center; gap: 15px; margin: 20px 0;">
        <a href="https://www.linkedin.com/in/jeffandy/" target="_blank">
            <img src="https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge&logo=linkedin&logoColor=white" alt="LinkedIn" />
        </a>
        <a href="https://youtube.com/playlist?list=PLQjGOmN1hO_lLIiEqIUR2SiACnGt3V59G&si=uulsP6Kxux7h0q7F" target="_blank">
            <img src="https://img.shields.io/badge/YouTube-FF0000?style=for-the-badge&logo=youtube&logoColor=white" alt="YouTube" />
        </a>
    </div>
    """, unsafe_allow_html=True)
    
    # st.markdown("---")

# ============================================
# YOUTUBE
# ============================================

    # Embedded YouTube video
    # st.markdown("## 🎥 100 Days Journey")
    st.markdown("""
    <iframe width="100%" height="400" 
    src="https://www.youtube.com/embed/HpwydaiR9ns?si=Rj3_dJjgJnGXsNqd" 
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
        Tracking my journey to become hireable as a Data Engineer & Developer through:
        - **CT (Coding Time):** Python, SQL, Data Engineering, Design (CSS, Javascript)
        - **VT (Video Time):** Documenting daily progress
        - **100 Days Challenge:** Building projects and skills
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
        
        # Get last week's data for comparison
        if len(weekly_summary) >= 2:
            last_week = weekly_summary.iloc[1]  # Most recent complete week
            ct_last_week = last_week['ct_hours']
            vt_last_week = last_week['vt_hours']
        elif not weekly_summary.empty:
            last_week = weekly_summary.iloc[0]
            ct_last_week = last_week['ct_hours']
            vt_last_week = last_week['vt_hours']
        else:
            ct_last_week = 0
            vt_last_week = 0
        
        # Calculate comparison to last week (not percentage of total)
        if ct_last_week > 0:
            ct_vs_last_week = ((ct_this_week - ct_last_week) / ct_last_week * 100)
        else:
            ct_vs_last_week = 0
            
        if vt_last_week > 0:
            vt_vs_last_week = ((vt_this_week - vt_last_week) / vt_last_week * 100)
        else:
            vt_vs_last_week = 0
        
        # Calculate CT:VT ratio for this week
        if total_this_week > 0:
            ct_pct = (ct_this_week / total_this_week * 100)
            vt_pct = (vt_this_week / total_this_week * 100)
        else:
            ct_pct = vt_pct = 0
        
        with col1:
            st.metric("⏱️ Total Hours", f"{total_this_week:.1f} hrs")
        
        with col2:
            ratio_color = "🟢" if 60 <= ct_pct <= 80 else "🟡" if 50 <= ct_pct <= 90 else "🔴"
            st.metric(f"{ratio_color} CT:VT Ratio", f"{ct_pct:.0f}:{vt_pct:.0f}")
            
        with col3:
            st.metric("💻 Coding Time", f"{ct_this_week:.1f} hrs", f"{ct_vs_last_week:+.0f}%")
        
        with col4:
            st.metric("🎥 Video Time", f"{vt_this_week:.1f} hrs", f"{vt_vs_last_week:+.0f}%")
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
        # This calculate actual challenge days (excluding Sabbaths)
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
            
            # This calculates actual challenge days completed (excluding Sabbaths)
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
                "ct_hours": st.column_config.NumberColumn("Coding Hours", format="%.1f"),
                "vt_hours": st.column_config.NumberColumn("Video Hours", format="%.1f"),
                "total_hours": st.column_config.NumberColumn("Total Hours", format="%.1f"),
                "ct_vt_ratio": "Coding:Video Ratio"
            },
            hide_index=True,
            use_container_width=True
        )
    
    # Footer
    st.markdown("---")
    st.markdown("""
    <div style='text-align: center; color: #666;'>
        <p>Built by Jeffandy St.Hubert | Data Engineer </p>
        <p> AllTogether Tech</p>
    </div>
    """, unsafe_allow_html=True)

    # ============================================
    # CODE VIEWER SECTION (TABBED)
    # ============================================
    
    st.markdown("---")
    st.markdown("## 🧑🏾‍💻 Original Code for this Data Pipeline & webapp")
    st.markdown("**Built on Day 27 of my 100 days of code, this data pipeline automatically keeps track of my learning while helping me set better weekly goals")
    
    # Helper function to render code with VS Code styling
    def render_code_viewer(code, filename):
        st.markdown(f'<div class="code-header">📄 {filename}</div>', unsafe_allow_html=True)
        
        code_lines = code.split('\n')
        code_html = '<div class="code-editor" style="white-space: pre; font-family: \'Courier New\', monospace;">'
        
        for i, line in enumerate(code_lines, 1):
            highlighted_line = line.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
            
            # Keywords
            for keyword in ['def ', 'while ', 'if ', 'elif ', 'else:', 'break', 'return', 'True:', 'False', 'import ', 'from ', 'in ', 'not ']:
                highlighted_line = highlighted_line.replace(keyword, f'<span class="keyword">{keyword}</span>')
            
            # Functions
            for func in ['print(', 'input(', 'range(', 'len(', 'list(']:
                highlighted_line = highlighted_line.replace(func, f'<span class="function">{func}</span>')
            
            # Methods
            for method in ['.lower()', '.keys()', '.append(', '.isalpha()', '.choice(']:
                highlighted_line = highlighted_line.replace(method, f'.<span class="function">{method[1:]}</span>')
            
            # Handle .join( and play_game()
            highlighted_line = highlighted_line.replace('.join(', '.<span class="function">join</span>(')
            highlighted_line = highlighted_line.replace('play_game()', '<span class="function">play_game</span>()')
            
            # Strings
            import re
            highlighted_line = re.sub(r'(".*?")', r'<span class="string">\1</span>', highlighted_line)
            highlighted_line = re.sub(r"('.*?')", r'<span class="string">\1</span>', highlighted_line)
            
            # Comments
            if '#' in highlighted_line and '<span' not in highlighted_line.split('#')[-1]:
                parts = highlighted_line.split('#', 1)
                if len(parts) == 2:
                    highlighted_line = parts[0] + '<span class="comment">#' + parts[1] + '</span>'
            
            code_html += f'<div style="display: flex;"><span class="line-number">{i:>3}</span><span>{highlighted_line}</span></div>'
        
        code_html += '</div>'
        st.markdown(code_html, unsafe_allow_html=True)
    
    # Create tabs for each file
    tab1, tab2, tab3 = st.tabs([
        "📱 ETL from Harvest App", 
        "☁️ Supabase Schema Setup", 
        "📊 Queries & Visualization"
    ])
    
    # Tab 1: Code for ETL from Harvest App
    with tab1:
        ETL_code = '''import pandas as pd
import numpy as np
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values
import re

# ============================================
# STEP 1: EXTRACT & TRANSFORM
# ============================================

def parse_harvest_csv(filepath):
    """
    Parse Harvest CSV and categorize time entries
    
    Returns:
        df: Transformed DataFrame with new categorization columns
    """
    # Read CSV
    df = pd.read_csv(filepath)
    
    print(f"📊 Loaded {len(df)} records from Harvest CSV")
    print(f"Date range: {df['Date'].min()} to {df['Date'].max()}")
    
    # Select only needed columns
    columns_to_keep = ['Date', 'Task', 'Notes', 'Hours']
    df = df[columns_to_keep].copy()
    
    # Convert date to proper format
    df['Date'] = pd.to_datetime(df['Date'])
    
    # ============================================
    # 100 DAYS CHALLENGE WEEK CALCULATION
    # ============================================
    # Official start: December 7, 2025 (SUNDAY - Day 1, Week 1)
    # Weeks run Sunday-Saturday (Sabbath on Saturday)
    # Week 1: Dec 7 (Sun) - Dec 13 (Sat), Sabbath Dec 13
    # Week 2: Dec 14 (Sun) - Dec 20 (Sat), Sabbath Dec 20
    # Week 3: Dec 21 (Sun) - Dec 27 (Sat), Sabbath Dec 27
    # Week 4: Dec 28 (Sun) - Jan 3 (Sat), Sabbath Jan 3
    # Week 5: Jan 4 (Sun) - Jan 10 (Sat), Sabbath Jan 10
    
    CHALLENGE_START = pd.Timestamp('2025-12-07')  # Sunday, Dec 7, 2025
    
    def calculate_challenge_week(date):
        """
        Calculate week number based on challenge start date.
        Week 1 starts Sunday Dec 7, 2025. Weeks are Sunday-Saturday.
        """
        if date < CHALLENGE_START:
            # For dates before challenge, use regular Sunday-based weeks
            # dayofweek: Monday=0, Sunday=6
            days_since_sunday = (date.dayofweek + 1) % 7
            week_start = date - pd.Timedelta(days=days_since_sunday)
            # Use regular ISO week number for pre-challenge dates
            week_number = date.isocalendar().week
            return week_start, week_number
        
        # Calculate days since challenge start (Sunday Dec 7)
        days_since_start = (date - CHALLENGE_START).days
        
        # Calculate which week (0-indexed, so add 1)
        week_number = (days_since_start // 7) + 1
        
        # Calculate week start (always a Sunday)
        # Week 1: Dec 7, Week 2: Dec 14, Week 3: Dec 21, etc.
        week_start = CHALLENGE_START + pd.Timedelta(days=(week_number - 1) * 7)
        
        return week_start, week_number
    
    # Apply the calculation
    df[['Week_Start', 'Week_Number']] = df['Date'].apply(
        lambda x: pd.Series(calculate_challenge_week(x))
    )
    df['Year'] = df['Date'].dt.year
    
    # ============================================
    # CATEGORIZATION LOGIC
    # ============================================
    
    # Define CT tasks (all MASTERY tasks)
    ct_tasks = [
        'MASTERY: Data Engineering Bootcamp',
        'MASTERY: Python Bootcamp',
        'MASTERY: SQL Bootcamp',
        'MASTERY: HackerRank SQL',
        'MASTERY: AWS BIG DATA bootcamp',
        'MASTERY: FODE',
        'MASTERY: design'
    ]
    
    # Define VT tasks (all BUILDING PROJECTS: Video tasks)
    vt_tasks = [
        'BUILDING PROJECTS: Video filming',
        'BUILDING PROJECTS: Video Script Writing',
        'BUILDING PROJECTS: Video editing'
    ]
    
    # Create time_type column (CT or VT or Other)
    def categorize_time_type(task):
        if pd.isna(task):
            return 'Other'
        if any(ct in task for ct in ct_tasks):
            return 'CT'
        elif any(vt in task for vt in vt_tasks):
            return 'VT'
        else:
            return 'Other'
    
    df['Time_Type'] = df['Task'].apply(categorize_time_type)
    
    # ============================================
    # CT CATEGORIZATION
    # ============================================
    
    def categorize_ct_task(task):
        """Categorize CT tasks by subject area"""
        if pd.isna(task):
            return None
        if 'SQL' in task or 'HackerRank SQL' in task:
            return 'SQL'
        elif 'Data Engineering' in task:
            return 'Data_Engineering'
        elif 'Python' in task:
            return 'Python'
        elif 'FODE' in task:
            return 'FODE'
        elif 'AWS' in task:
            return 'AWS'
        elif 'design' in task or 'CSS' in task or 'Javascript' in task:
            return 'design
        else:
            return None
    
    df['CT_Category'] = df['Task'].apply(categorize_ct_task)
    
    # ============================================
    # CT SUB-CATEGORIZATION (Deep Dive vs Shipping)
    # ============================================
    
    def categorize_ct_type(row):
        """
        Determine if CT is Deep Dive or Shipping
        Post 12/31/2025: Look for DEEP DIVE or SHIPPING keywords in notes
        Pre 12/31/2025: Return 'Uncategorized'
        """
        if row['Time_Type'] != 'CT':
            return None
        
        if pd.isna(row['Notes']):
            # If no notes and it's after 12/31, default to Uncategorized
            if row['Date'] >= pd.Timestamp('2025-12-31'):
                return 'Uncategorized'
            else:
                return 'Pre_Classification'
        
        notes = str(row['Notes'])
        
        # Post 12/31 logic - look for explicit keywords
        if row['Date'] >= pd.Timestamp('2025-12-31'):
            if 'DEEP DIVE' in notes or 'DL:' in notes or 'DL ' in notes:
                return 'Deep_Dive'
            elif 'SHIPPING' in notes or 'S:' in notes or notes.startswith('S '):
                return 'Shipping'
            else:
                return 'Uncategorized'
        else:
            # Pre 12/31 - mark for potential manual categorization
            return 'Pre_Classification'
    
    df['CT_Type'] = df.apply(categorize_ct_type, axis=1)
    
    # ============================================
    # VT CATEGORIZATION
    # ============================================
    
    def categorize_vt_task(task):
        """Categorize VT tasks by video activity type"""
        if pd.isna(task):
            return None
        task_lower = task.lower()
        if 'filming' in task_lower:
            return 'Filming'
        elif 'script' in task_lower:
            return 'Scripting'
        elif 'editing' in task_lower:
            return 'Editing'
        else:
            return None
    
    df['VT_Category'] = df['Task'].apply(categorize_vt_task)
    
    # ============================================
    # EXTRACT DAY NUMBER FROM VT NOTES
    # ============================================
    
    def extract_day_number(row):
        """
        Extract day number from VT task or notes
        Examples: "Day 7", "day 7", "day7", "Day7"
        """
        if row['Time_Type'] != 'VT':
            return None
        
        # Check both Task and Notes
        text_to_search = str(row['Task']) + ' ' + str(row['Notes'])
        
        if pd.isna(text_to_search):
            return None
        
        # Look for "Day X" or "day X" pattern (with or without space)
        match = re.search(r'[Dd]ay\s*(\d+)', text_to_search)
        if match:
            return int(match.group(1))
        
        return None
    
    df['Day_Number'] = df.apply(extract_day_number, axis=1)
    
    # Clean up Notes column (keep original but add cleaned version)
    df['Notes_Clean'] = df['Notes'].fillna('').str.strip()
    
    # Add derived columns for easier analysis
    df['Month'] = df['Date'].dt.to_period('M')
    df['Day_of_Week'] = df['Date'].dt.day_name()
    
    print(f"\n✅ Transformation complete:")
    print(f"   - CT entries: {len(df[df['Time_Type'] == 'CT'])}")
    print(f"   - VT entries: {len(df[df['Time_Type'] == 'VT'])}")
    print(f"   - Other entries: {len(df[df['Time_Type'] == 'Other'])}")
    
    return df


# ============================================
# STEP 2: GENERATE SUMMARY STATISTICS
# ============================================

def generate_weekly_summary(df):
    """
    Generate weekly summary statistics with CT:VT ratios
    """
    print("\n📈 Generating weekly summary...")
    
    # Filter to only include CT and VT (exclude 'Other')
    df_filtered = df[df['Time_Type'].isin(['CT', 'VT'])].copy()
    
    # Group by week and time type
    weekly = df_filtered.groupby(['Week_Start', 'Time_Type']).agg({
        'Hours': 'sum'
    }).reset_index()
    
    # Pivot to get CT and VT as columns
    weekly_pivot = weekly.pivot(index='Week_Start', columns='Time_Type', values='Hours').fillna(0)
    
    # Calculate total and ratios
    weekly_pivot['Total_Hours'] = weekly_pivot.sum(axis=1)
    
    if 'CT' in weekly_pivot.columns and 'VT' in weekly_pivot.columns:
        weekly_pivot['CT_Percentage'] = (weekly_pivot['CT'] / weekly_pivot['Total_Hours'] * 100).round(1)
        weekly_pivot['VT_Percentage'] = (weekly_pivot['VT'] / weekly_pivot['Total_Hours'] * 100).round(1)
        weekly_pivot['CT_VT_Ratio'] = (weekly_pivot['CT_Percentage'].astype(int).astype(str) + ':' + 
                                        weekly_pivot['VT_Percentage'].astype(int).astype(str))
    
    # Reset index to make Week_Start a column
    weekly_pivot = weekly_pivot.reset_index()
    weekly_pivot['Week_Start'] = weekly_pivot['Week_Start'].dt.date
    
    return weekly_pivot


def generate_ct_breakdown(df):
    """
    Generate CT breakdown by category (SQL, Python, Data Engineering, etc.)
    """
    print("\n📊 Generating CT category breakdown...")
    
    ct_df = df[df['Time_Type'] == 'CT'].copy()
    
    if len(ct_df) == 0:
        print("   ⚠️  No CT entries found")
        return pd.DataFrame(), pd.DataFrame()
    
    # By category (SQL, Python, Data Engineering, etc.)
    ct_category = ct_df.groupby(['Week_Start', 'CT_Category']).agg({
        'Hours': 'sum'
    }).reset_index()
    ct_category['Week_Start'] = ct_category['Week_Start'].dt.date
    
    # By type (Deep Dive vs Shipping) - only for post 12/31 data
    ct_type = ct_df[ct_df['Date'] >= '2025-12-31'].groupby(['Week_Start', 'CT_Type']).agg({
        'Hours': 'sum'
    }).reset_index()
    
    if len(ct_type) > 0:
        ct_type['Week_Start'] = ct_type['Week_Start'].dt.date
        
        # Calculate Deep Dive vs Shipping ratio
        ct_type_pivot = ct_type.pivot(index='Week_Start', columns='CT_Type', values='Hours').fillna(0)
        
        if 'Deep_Dive' in ct_type_pivot.columns and 'Shipping' in ct_type_pivot.columns:
            ct_type_pivot['Total_CT'] = ct_type_pivot['Deep_Dive'] + ct_type_pivot['Shipping']
            ct_type_pivot['DD_Percentage'] = (ct_type_pivot['Deep_Dive'] / ct_type_pivot['Total_CT'] * 100).round(1)
            ct_type_pivot['S_Percentage'] = (ct_type_pivot['Shipping'] / ct_type_pivot['Total_CT'] * 100).round(1)
            # Handle NaN values before converting to int
            ct_type_pivot['DD_S_Ratio'] = ct_type_pivot.apply(
                lambda row: f"{int(row['DD_Percentage'])}:{int(row['S_Percentage'])}" 
                if pd.notna(row['DD_Percentage']) and pd.notna(row['S_Percentage']) 
                else 'N/A', 
                axis=1
            )
        
        ct_type = ct_type_pivot.reset_index()
    
    return ct_category, ct_type


def generate_vt_breakdown(df):
    """
    Generate VT breakdown by category (Filming, Scripting, Editing)
    """
    print("\n🎥 Generating VT category breakdown...")
    
    vt_df = df[df['Time_Type'] == 'VT'].copy()
    
    if len(vt_df) == 0:
        print("   ⚠️  No VT entries found")
        return pd.DataFrame()
    
    vt_category = vt_df.groupby(['Week_Start', 'VT_Category']).agg({
        'Hours': 'sum'
    }).reset_index()
    vt_category['Week_Start'] = vt_category['Week_Start'].dt.date
    
    return vt_category


def generate_100_days_progress(df):
    """
    Track progress through 100 Days to Hireable challenge
    """
    print("\n🎯 Generating 100 Days progress...")
    
    # Filter to entries with day numbers
    days_df = df[df['Day_Number'].notna()].copy()
    
    if len(days_df) == 0:
        print("   ⚠️  No day numbers found in VT entries")
        return pd.DataFrame()
    
    # Get unique days and their dates
    progress = days_df.groupby('Day_Number').agg({
        'Date': 'min',  # First occurrence of each day
        'Hours': 'sum'  # Total hours spent on that day's content
    }).reset_index()
    
    progress = progress.sort_values('Day_Number')
    progress.columns = ['Day', 'Date', 'Total_Hours']
    
    # Calculate cumulative progress
    progress['Days_Completed'] = progress['Day']
    progress['Days_Remaining'] = 100 - progress['Day']
    progress['Progress_Percentage'] = (progress['Day'] / 100 * 100).round(1)
    
    return progress


# ============================================
# STEP 3: LOAD TO POSTGRESQL
# ============================================

def create_harvest_table(conn):
    """
    Create PostgreSQL table for harvest data
    """
    create_table_query = """
    CREATE TABLE IF NOT EXISTS harvest_time_tracking (
        id SERIAL PRIMARY KEY,
        date DATE NOT NULL,
        week_start DATE NOT NULL,
        week_number INTEGER,
        year INTEGER,
        month VARCHAR(7),
        day_of_week VARCHAR(10),
        task VARCHAR(255),
        notes TEXT,
        hours DECIMAL(5,2) NOT NULL,
        time_type VARCHAR(20),          -- 'CT', 'VT', or 'Other'
        ct_category VARCHAR(50),         -- 'SQL', 'Python', 'Data_Engineering', 'FODE', 'AWS'
        vt_category VARCHAR(50),         -- 'Filming', 'Scripting', 'Editing'
        ct_type VARCHAR(50),             -- 'Deep_Dive', 'Shipping', 'Uncategorized', 'Pre_Classification'
        day_number INTEGER,              -- For VT: which day of 100
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(date, task, notes)        -- Prevent duplicate entries
    );
    
    CREATE INDEX IF NOT EXISTS idx_date ON harvest_time_tracking(date);
    CREATE INDEX IF NOT EXISTS idx_week_start ON harvest_time_tracking(week_start);
    CREATE INDEX IF NOT EXISTS idx_time_type ON harvest_time_tracking(time_type);
    CREATE INDEX IF NOT EXISTS idx_ct_category ON harvest_time_tracking(ct_category);
    CREATE INDEX IF NOT EXISTS idx_day_number ON harvest_time_tracking(day_number);
    """
    
    with conn.cursor() as cur:
        cur.execute(create_table_query)
        conn.commit()
    
    print("✅ Table created/verified successfully")


def load_to_postgres(df, conn, truncate=False):
    """
    Load transformed dataframe to PostgreSQL
    
    Args:
        df: DataFrame to load
        conn: PostgreSQL connection
        truncate: If True, clear existing data before loading
    """
    if truncate:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE harvest_time_tracking RESTART IDENTITY CASCADE;")
            conn.commit()
        print("⚠️  Existing data truncated")
    
    # Prepare data for insertion
    df_subset = df[[
        'Date', 'Week_Start', 'Week_Number', 'Year', 'Month', 
        'Day_of_Week', 'Task', 'Notes_Clean', 'Hours',
        'Time_Type', 'CT_Category', 'VT_Category', 'CT_Type', 'Day_Number'
    ]].copy()
    
    # Convert Period to string for Month column
    df_subset['Month'] = df_subset['Month'].astype(str)
    
    # Convert integer columns FIRST (while NaN detection works)
    for col in ['Week_Number', 'Year', 'Day_Number']:
        if col in df_subset.columns:
            df_subset[col] = df_subset[col].apply(
                lambda x: int(x) if pd.notna(x) else None
            )
    
    # NUCLEAR OPTION: Build list manually row by row
    # This is the ONLY way to prevent pandas from converting None back to NaN
    import numpy as np
    
    def to_python_type(val):
        """Convert numpy types to Python native types for psycopg2"""
        if pd.isna(val):
            return None
        elif isinstance(val, (np.integer, np.int64, np.int32)):
            return int(val)
        elif isinstance(val, (np.floating, np.float64, np.float32)):
            return float(val)
        elif isinstance(val, np.bool_):
            return bool(val)
        elif isinstance(val, np.str_):
            return str(val)
        else:
            return val
    
    values = []
    for idx in range(len(df_subset)):
        row = []
        for col in df_subset.columns:
            val = df_subset[col].iloc[idx]
            row.append(to_python_type(val))
        values.append(tuple(row))
    
    # Insert query with ON CONFLICT to handle duplicates
    insert_query = """
    INSERT INTO harvest_time_tracking 
    (date, week_start, week_number, year, month, day_of_week, 
     task, notes, hours, time_type, ct_category, vt_category, ct_type, day_number)
    VALUES %s
    ON CONFLICT (date, task, notes) DO UPDATE SET
        hours = EXCLUDED.hours,
        time_type = EXCLUDED.time_type,
        ct_category = EXCLUDED.ct_category,
        vt_category = EXCLUDED.vt_category,
        ct_type = EXCLUDED.ct_type,
        day_number = EXCLUDED.day_number
    """
    
    with conn.cursor() as cur:
        execute_values(cur, insert_query, values)
        conn.commit()
    
    print(f"✅ Loaded/updated {len(values)} rows to PostgreSQL")


# ============================================
# MAIN EXECUTION
# ============================================

def main():
    """
    Main ETL pipeline execution
    """
    print("="*60)
    print("HARVEST TIME TRACKING ETL PIPELINE")
    print("="*60)
    
    # Configuration
    # Use relative path that works both locally and in GitHub Actions
    import os
    HARVEST_CSV_PATH = os.path.join(os.path.dirname(__file__), 'harvest_time_report.csv')
    
    # PostgreSQL configuration
    # Uses environment variables for deployment, with local fallback
    
    DB_CONFIG = {
        'dbname': os.getenv('DB_NAME', 'harvest_tracker'),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD', 'password'),
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': os.getenv('DB_PORT', '5432')
    }
    
    print(f"🔌 Connecting to database at {DB_CONFIG['host']}...")
    
    # ============================================
    # STEP 1: EXTRACT & TRANSFORM
    # ============================================
    print("\n" + "="*60)
    print("STEP 1: EXTRACT & TRANSFORM")
    print("="*60)
    
    df = parse_harvest_csv(HARVEST_CSV_PATH)
    
    # Save transformed CSV for inspection (optional - only if outputs dir exists or can be created)
    script_dir = os.path.dirname(__file__)
    outputs_dir = os.path.join(script_dir, 'outputs')
    
    # Create outputs directory if it doesn't exist (for local use)
    try:
        os.makedirs(outputs_dir, exist_ok=True)
        save_outputs = True
    except:
        # If we can't create outputs (like in GitHub Actions), skip saving CSVs
        save_outputs = False
        print("\n⚠️  Outputs directory not available - skipping CSV exports (database loading will continue)")
    
    if save_outputs:
        output_csv = os.path.join(outputs_dir, 'harvest_transformed.csv')
        df.to_csv(output_csv, index=False)
        print(f"\n✅ Transformed CSV saved to: {output_csv}")
    
    # ============================================
    # STEP 2: GENERATE SUMMARIES
    # ============================================
    print("\n" + "="*60)
    print("STEP 2: GENERATE SUMMARIES")
    print("="*60)
    
    # Weekly CT:VT summary
    weekly_summary = generate_weekly_summary(df)
    if save_outputs:
        weekly_csv = os.path.join(outputs_dir, 'weekly_summary.csv')
        weekly_summary.to_csv(weekly_csv, index=False)
    print(f"\n📊 Weekly Summary (last 5 weeks):")
    print(weekly_summary.tail(5).to_string(index=False))
    if save_outputs:
        print(f"\n✅ Saved to: {weekly_csv}")
    
    # CT breakdown by category
    ct_category, ct_type = generate_ct_breakdown(df)
    if len(ct_category) > 0:
        if save_outputs:
            ct_cat_csv = os.path.join(outputs_dir, 'ct_category_breakdown.csv')
            ct_category.to_csv(ct_cat_csv, index=False)
        print(f"\n📊 CT Category Breakdown (last 5 weeks):")
        print(ct_category.tail(10).to_string(index=False))
        if save_outputs:
            print(f"\n✅ Saved to: {ct_cat_csv}")
    
    if len(ct_type) > 0:
        if save_outputs:
            ct_type_csv = os.path.join(outputs_dir, 'ct_type_breakdown.csv')
            ct_type.to_csv(ct_type_csv, index=False)
        print(f"\n📊 Deep Dive vs Shipping Breakdown (post 12/31):")
        print(ct_type.to_string(index=False))
        if save_outputs:
            print(f"\n✅ Saved to: {ct_type_csv}")
    
    # VT breakdown
    vt_category = generate_vt_breakdown(df)
    if len(vt_category) > 0:
        if save_outputs:
            vt_cat_csv = os.path.join(outputs_dir, 'vt_category_breakdown.csv')
            vt_category.to_csv(vt_cat_csv, index=False)
        print(f"\n🎥 VT Category Breakdown (last 5 weeks):")
        print(vt_category.tail(10).to_string(index=False))
        if save_outputs:
            print(f"\n✅ Saved to: {vt_cat_csv}")
    
    # 100 Days progress
    progress_100days = generate_100_days_progress(df)
    if len(progress_100days) > 0:
        if save_outputs:
            progress_csv = os.path.join(outputs_dir, '100_days_progress.csv')
            progress_100days.to_csv(progress_csv, index=False)
        print(f"\n🎯 100 Days to Hireable Progress:")
        print(progress_100days.tail(10).to_string(index=False))
        if save_outputs:
            print(f"\n✅ Saved to: {progress_csv}")
    
    # ============================================
    # STEP 3: LOAD TO POSTGRESQL
    # ============================================
    print("\n" + "="*60)
    print("STEP 3: LOAD TO POSTGRESQL")
    print("="*60)
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("Connected to PostgreSQL")
        
        create_harvest_table(conn)
        
        # Load data (set truncate=True to clear existing data first)
        load_to_postgres(df, conn, truncate=False)
        
        conn.close()
        print("✅ Database operations completed")
        
    except psycopg2.OperationalError as e:
        print(f"\n⚠️ Could not connect to PostgreSQL: {e}")
        print("The CSV transformations were successful. Update DB_CONFIG and run again to load to PostgreSQL.")
    except Exception as e:
        print(f"❌ Database error: {e}")
    
    # ============================================
    # SUMMARY
    # ============================================
    print("\n" + "="*60)
    print("PIPELINE COMPLETE")
    print("="*60)
    print("\nGenerated files:")
    print("  1. harvest_transformed.csv - Full transformed dataset")
    print("  2. weekly_summary.csv - Weekly CT:VT ratios")
    print("  3. ct_category_breakdown.csv - CT by subject (SQL, Python, etc.)")
    print("  4. ct_type_breakdown.csv - Deep Dive vs Shipping breakdown")
    print("  5. vt_category_breakdown.csv - Video work breakdown")
    print("  6. 100_days_progress.csv - 100 Days challenge tracker")
    print("\nNext steps:")
    print("  1. Review the generated CSV files")
    print("  2. Update DB_CONFIG with your PostgreSQL credentials")
    print("  3. Run again to load to database")
    print("  4. Build Streamlit dashboard to visualize this data")


if __name__ == "__main__":
    main()
'''
        render_code_viewer(ETL_code, "harvest_etl.py")
    
    # Tab 2: Code for Database Setup
    with tab2:
        database_setup = """DROP TABLE IF EXISTS harvest_time_tracking CASCADE;

CREATE TABLE harvest_time_tracking (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    week_start DATE NOT NULL,
    week_number BIGINT,              -- Changed from INTEGER to BIGINT
    year BIGINT,                     -- Changed from INTEGER to BIGINT
    month VARCHAR(7),
    day_of_week VARCHAR(10),
    task VARCHAR(255),
    notes TEXT,
    hours DECIMAL(5,2) NOT NULL CHECK (hours >= 0),
    time_type VARCHAR(20),           -- 'CT', 'VT', or 'Other'
    ct_category VARCHAR(50),         -- 'SQL', 'Python', 'Data_Engineering'
    vt_category VARCHAR(50),         -- 'Filming', 'Scripting', 'Editing'
    ct_type VARCHAR(50),             -- 'Deep_Dive', 'Shipping', 'Uncategorized', 'Pre_Classification'
    day_number BIGINT,               -- Changed from INTEGER to BIGINT
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Prevent duplicate entries
    CONSTRAINT unique_entry UNIQUE(date, task, notes)
);

-- Create indexes for faster queries
CREATE INDEX idx_date ON harvest_time_tracking(date);
CREATE INDEX idx_week_start ON harvest_time_tracking(week_start);
CREATE INDEX idx_time_type ON harvest_time_tracking(time_type);
CREATE INDEX idx_ct_category ON harvest_time_tracking(ct_category) WHERE ct_category IS NOT NULL;
CREATE INDEX idx_ct_type ON harvest_time_tracking(ct_type) WHERE ct_type IS NOT NULL;
CREATE INDEX idx_day_number ON harvest_time_tracking(day_number) WHERE day_number IS NOT NULL;
CREATE INDEX idx_year_week ON harvest_time_tracking(year, week_number);

-- Add comments for documentation
COMMENT ON TABLE harvest_time_tracking IS 'Main table for tracking time entries from Harvest app';
COMMENT ON COLUMN harvest_time_tracking.time_type IS 'CT = Coding Time, VT = Video Time, Other = Everything else';
COMMENT ON COLUMN harvest_time_tracking.ct_category IS 'Subject area for coding: SQL, Python, Data_Engineering';
COMMENT ON COLUMN harvest_time_tracking.ct_type IS 'Deep_Dive = Learning, Shipping = Building projects (tracked after 2025-12-31)';
COMMENT ON COLUMN harvest_time_tracking.day_number IS 'Day number for video content (not challenge day)';

-- ============================================
-- SUCCESS MESSAGE
-- ============================================

DO $$
BEGIN
    RAISE NOTICE '✅ Database schema created successfully!';
    RAISE NOTICE 'Table created: harvest_time_tracking';
    RAISE NOTICE 'Data types: week_number, year, day_number = BIGINT';
    RAISE NOTICE '';
    RAISE NOTICE 'Next steps:';
    RAISE NOTICE '1. Run your ETL script to load data';
    RAISE NOTICE '2. Verify data: SELECT COUNT(*) FROM harvest_time_tracking;';
    RAISE NOTICE '3. Check weeks: SELECT DISTINCT week_number, week_start FROM harvest_time_tracking ORDER BY week_number;';
END $$;
"""
        render_code_viewer(database_setup, "setup_database.sql")
    
    # Tab 3: Code for Query and Visualization
    with tab3:
        Queries_Visualization_sample = '''import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import os

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
                dbname=os.getenv('DB_NAME', 'the_name_of_my_DB'),
                user=os.getenv('DB_USER', 'the_name_of_my_user'),
                password=os.getenv('DB_PASSWORD', 'the_PW_for_my_DB'),
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
    Load weekly CT:VT summary for 100 Days Challenge"""
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
'''
        render_code_viewer(Queries_Visualization_sample, "streamlit_app.py")
    

if __name__ == "__main__":
    main()
