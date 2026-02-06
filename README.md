# 100 Days to Hireable Tracker

Real-time ETL pipeline and dashboard tracking my journey to become a hireable Data Engineer.

https://100daytracker.streamlit.app/

## Live Dashboard

[View Live Dashboard →]([MY_STREAMLIT_URL](https://100daytracker.streamlit.app/))

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
