import pandas as pd
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
        'MASTERY: FODE'
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
    print("🚀 HARVEST TIME TRACKING ETL PIPELINE")
    print("="*60)
    
    # Configuration
    HARVEST_CSV_PATH = '/Users/jeffandyalltogether/Documents/AllTogether Tech/100DAYS_projects/100-days-tracker/harvest_time_report.csv'
    
    # PostgreSQL configuration
    # Uses environment variables for deployment, with local fallback
    import os
    
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
    
    # Save transformed CSV for inspection
    output_csv = '/Users/jeffandyalltogether/Documents/AllTogether Tech/100DAYS_projects/100-days-tracker/outputs/harvest_transformed.csv'
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
    weekly_csv = '/Users/jeffandyalltogether/Documents/AllTogether Tech/100DAYS_projects/100-days-tracker/outputs/weekly_summary.csv'
    weekly_summary.to_csv(weekly_csv, index=False)
    print(f"\n📊 Weekly Summary (last 5 weeks):")
    print(weekly_summary.tail(5).to_string(index=False))
    print(f"\n✅ Saved to: {weekly_csv}")
    
    # CT breakdown by category
    ct_category, ct_type = generate_ct_breakdown(df)
    if len(ct_category) > 0:
        ct_cat_csv = '/Users/jeffandyalltogether/Documents/AllTogether Tech/100DAYS_projects/100-days-tracker/outputs/ct_category_breakdown.csv'
        ct_category.to_csv(ct_cat_csv, index=False)
        print(f"\n📊 CT Category Breakdown (last 5 weeks):")
        print(ct_category.tail(10).to_string(index=False))
        print(f"\n✅ Saved to: {ct_cat_csv}")
    
    if len(ct_type) > 0:
        ct_type_csv = '/Users/jeffandyalltogether/Documents/AllTogether Tech/100DAYS_projects/100-days-tracker/outputs/ct_type_breakdown.csv'
        ct_type.to_csv(ct_type_csv, index=False)
        print(f"\n📊 Deep Dive vs Shipping Breakdown (post 12/31):")
        print(ct_type.to_string(index=False))
        print(f"\n✅ Saved to: {ct_type_csv}")
    
    # VT breakdown
    vt_category = generate_vt_breakdown(df)
    if len(vt_category) > 0:
        vt_cat_csv = '/Users/jeffandyalltogether/Documents/AllTogether Tech/100DAYS_projects/100-days-tracker/outputs/vt_category_breakdown.csv'
        vt_category.to_csv(vt_cat_csv, index=False)
        print(f"\n🎥 VT Category Breakdown (last 5 weeks):")
        print(vt_category.tail(10).to_string(index=False))
        print(f"\n✅ Saved to: {vt_cat_csv}")
    
    # 100 Days progress
    progress_100days = generate_100_days_progress(df)
    if len(progress_100days) > 0:
        progress_csv = '/Users/jeffandyalltogether/Documents/AllTogether Tech/100DAYS_projects/100-days-tracker/outputs/100_days_progress.csv'
        progress_100days.to_csv(progress_csv, index=False)
        print(f"\n🎯 100 Days to Hireable Progress:")
        print(progress_100days.tail(10).to_string(index=False))
        print(f"\n✅ Saved to: {progress_csv}")
    
    # ============================================
    # STEP 3: LOAD TO POSTGRESQL
    # ============================================
    print("\n" + "="*60)
    print("STEP 3: LOAD TO POSTGRESQL")
    print("="*60)
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("🔌 Connected to PostgreSQL")
        
        create_harvest_table(conn)
        
        # Load data (set truncate=True to clear existing data first)
        load_to_postgres(df, conn, truncate=False)
        
        conn.close()
        print("✅ Database operations completed")
        
    except psycopg2.OperationalError as e:
        print(f"\n⚠️  Could not connect to PostgreSQL: {e}")
        print("💡 The CSV transformations were successful. Update DB_CONFIG and run again to load to PostgreSQL.")
    except Exception as e:
        print(f"❌ Database error: {e}")
    
    # ============================================
    # SUMMARY
    # ============================================
    print("\n" + "="*60)
    print("✨ PIPELINE COMPLETE!")
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
