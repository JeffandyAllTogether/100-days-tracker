# 100 Days to Hireable Tracker

Real-time ETL pipeline and dashboard tracking my journey to become a hireable Data Engineer.

[![Streamlit App](https:)](MY_STREAMLIT_URL)

## Live Dashboard

[View Live Dashboard →](MY_STREAMLIT_URL)

## Architecture

```
Harvest API → CSV Export → Python ETL → PostgreSQL (Supabase) → Streamlit Dashboard
                              ↓
                        GitHub Actions
                      (Daily automation)
```

## Tech Stack

- **ETL Pipeline:** Python, Pandas, NumPy
- **Database:** PostgreSQL (Supabase)
- **Visualization:** Streamlit, Plotly
- **Automation:** GitHub Actions
- **Deployment:** Streamlit Cloud

## Features

- **Automated ETL Pipeline** processing 368+ time tracking entries
- **Custom Challenge Week Calculation** (Sunday-Saturday, excluding Sabbaths)
- **CT:VT Ratio Tracking** with 70:30 target visualization
- **Deep Dive vs Shipping Metrics** for learning approach analysis
- **5+ Interactive Visualizations** with real-time updates

## Deployment Guide

### 1. Set Up Supabase (Database)

1. Create account at [supabase.com](https://supabase.com)
2. Create new project: `100-days-tracker`
3. Get connection details from Settings → Database
4. Run the SQL schema in Supabase SQL Editor:
   ```sql
   -- Copy content from setup_database.sql
   ```

### 2. Deploy to Streamlit Cloud

1. Fork/clone this repository
2. Go to [share.streamlit.io](https://share.streamlit.io)
3. Click "New app" and select this repo
4. Add secrets in app settings:
   ```toml
   [database]
   dbname = "postgres"
   user = "postgres"
   password = "YOUR_SUPABASE_PASSWORD"
   host = "db.xxxxx.supabase.co"
   port = "5432"
   ```
5. Deploy!

### 3. Set Up GitHub Actions (Optional - for automation)

1. Add repository secrets in GitHub:
   - `DB_NAME`, `DB_USER`, `DB_PASSWORD`, `DB_HOST`, `DB_PORT`
2. Add `harvest_time_report.csv` to repository
3. Commit and push - GitHub Actions will run daily at 11:59 PM EST

## 💻 Local Development

### Prerequisites

- Python 3.12+
- PostgreSQL (or Supabase account)

### Installation

```bash
# Clone repository
git clone https://github.com/YOUR_USERNAME/100-days-tracker.git
cd 100-days-tracker

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Create .streamlit/secrets.toml with your database credentials
mkdir .streamlit
cp secrets.toml .streamlit/secrets.toml
# Edit .streamlit/secrets.toml with your credentials

# Run Streamlit app
streamlit run streamlit_app.py

# Run ETL pipeline
python harvest_etl.py
```

### Local Database Setup

If using local PostgreSQL instead of Supabase:

```bash
# Create database
createdb harvest_tracker

# Run schema
psql harvest_tracker < setup_database.sql

# Update .streamlit/secrets.toml with local credentials
```

## 📁 Project Structure

```
100-days-tracker/
├── streamlit_app.py          # Dashboard application
├── harvest_etl.py             # ETL pipeline script
├── requirements.txt           # Python dependencies
├── secrets.toml              # Secrets file
├── .github/
│   └── workflows/
│       └── update-data.yml   # GitHub Actions workflow
├── .gitignore
└── README.md
```

## 🎯 Key Metrics Tracked

- **Coding Time (CT):** Python, SQL, Data Engineering
- **Video Time (VT):** Filming, Editing, Scripting
- **Weekly CT:VT Ratio:** Target 70:30
- **Deep Dive vs Shipping:** Target 30:70
- **100 Days Progress:** Days completed (excluding Sabbaths)

## Learning Journey

WHY DID I MAKE THIS???
This project demonstrates:
- End-to-end data pipeline architecture
- ETL design with pandas (handling NaN/NULL conversions)
- PostgreSQL database modeling
- Real-time data visualization
- Cloud deployment and CI/CD automation
- Version control best practices

## Data Update Workflow

1. Export latest time data from Harvest as CSV
2. Commit CSV to repository (or use Harvest API in future)
3. GitHub Actions automatically runs ETL pipeline
4. Database updates on Supabase
5. Streamlit dashboard reflects changes (refresh browser)

## Technical Challenges Solved

- **Pandas NaN to PostgreSQL NULL conversion** - Custom type handling at DataFrame-to-database boundary
- **Custom week numbering** - Sunday-Saturday weeks starting Dec 7, 2025
- **Sabbath exclusion** - Filtering Saturday data from challenge day count
- **Cloud database integration** - Seamless local/cloud environment switching

## Contributing

This is a personal learning project, but suggestions and feedback are welcome!

## Contact

**Fandy Jeffandy**
- Portfolio: [[MY_PORTFOLIO](https://share.streamlit.io/user/jeffandyalltogether)]
- LinkedIn: [[MY_LINKEDIN](https://www.linkedin.com/in/jeffandy/)]
- GitHub: [@JeffandyAllTogether](https://github.com/JeffandyAllTogether)]

## License

Feel free to use this as a template for your own learning tracker!

---

Built as part of my 100 Days Coding challenge
# Updated preview
