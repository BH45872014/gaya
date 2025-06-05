# app.py
# Requirements:
#   pip install streamlit pandas numpy plotly statsmodels

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
from statsmodels.tsa.holtwinters import ExponentialSmoothing
from datetime import datetime
from calendar import monthrange

# 1) Page config & global CSS
st.set_page_config(
    page_title="MITR Dashboard",
    page_icon="📈",
    layout="wide",
)
st.markdown("""
<style>
  /* Hide the Streamlit menu */
  #MainMenu { visibility: hidden; }

  /* Orange gradient header */
  .full-header {
    background: linear-gradient(90deg, #FF7043, #FF8F00);
    padding: 12px 20px;
    border-radius: 0 0 8px 8px;
    margin-bottom: 8px;
    border: 2px solid #BF360C;
  }
  .full-header h1 {
    color: white; margin: 0; font-size: 1.8rem; font-weight: 600;
    letter-spacing: 1px;
  }

  /* Filter banner */
  .filter-banner {
    background: linear-gradient(90deg, #E3F2FD, #BBDEFB);
    padding: 16px;
    border-radius: 8px;
    margin-bottom: 16px;
    border: 2px solid #1A237E;
  }

  /* Indicator banner */
  .indicator-banner {
    background: #FFF3E0;
    padding: 16px;
    border-radius: 8px;
    margin-top: 24px;
    border: 2px solid #E65100;
  }

  /* Artifact card border */
  .artifact-card {
    padding: 16px;
    border-radius: 8px;
    border: 2px solid #333333;
    background-color: white;
    margin-bottom: 24px;
  }

  /* Darker buttons */
  .stButton>button {
    background-color: #E65100 !important;
    color: white !important;
    border-radius: 6px;
    border: 2px solid #BF360C !important;
  }
  .stSelectbox>div {
    border: 2px solid #BF360C !important;
    border-radius: 6px;
  }

  /* App background */
  [data-testid="stAppViewContainer"] { background-color: #F5F5F5; }
</style>
""", unsafe_allow_html=True)

# 2) Header
st.markdown(
    '<div class="full-header"><h1>📈 MITR - Machine Intelligence
Transformation & Reporting</h1></div>',
    unsafe_allow_html=True
)

# 3) Load data
@st.cache_data
def load_balance_data(path="balance_sheet_data.csv"):
    df = pd.read_csv(path, parse_dates=['date'], dayfirst=True)
    df['month'] = df['date'].dt.month_name()
    df['year']  = df['date'].dt.year
    return df

df = load_balance_data()

# 4) Simulate market indicators
dates_all = pd.date_range(df.date.min(), df.date.max(), freq="D")
df_fx = pd.DataFrame({
    "date": dates_all,
    "fx_volatility": np.random.uniform(0.5, 2.5, len(dates_all)).round(2),
    "interest_rate": np.random.uniform(1.0, 3.0, len(dates_all)).round(2)
})

# 5) Filter row wrapped in banner
st.markdown('<div class="filter-banner">', unsafe_allow_html=True)
cols = st.columns([1.5, 2, 2, 2, 1.5])
search = cols[0].text_input("🔍 Search Book")
books = sorted(df['book'].unique())
if search:
    books = [b for b in books if b.lower().startswith(search.lower())]
book = cols[1].selectbox("Select Book", books)

months = sorted(df['month'].unique(),
                key=lambda m: datetime.strptime(m, "%B").month)
current_mon = datetime.now().strftime("%B")
month = cols[2].selectbox(
    "Select Month", months,
    index=months.index(current_mon) if current_mon in months else 0
)

years = sorted(df['year'].unique())
current_year = datetime.now().year
year = cols[3].selectbox(
    "Select Year", years,
    index=years.index(current_year) if current_year in years else 0
)

predict = cols[4].button("Forecast")
st.markdown('</div>', unsafe_allow_html=True)
st.markdown("---")

# 6) Main content
df_f = df[(df.book == book) & (df.month == month) & (df.year == year)]
if df_f.empty:
    st.warning("No data for this selection.")
else:
    # KPIs
    st.markdown('<div class="artifact-card">', unsafe_allow_html=True)
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Total Booked", int(df_f['balance'].sum()))
    k2.metric("Avg. Balance", f"${df_f['balance'].mean():,.0f}")
    k3.metric("Total Journals", int(df_f['totaljournals'].sum()))
    k4.metric("Last Updated By", df_f['lastupdatedby'].iloc[-1])
    st.markdown('</div>', unsafe_allow_html=True)

    # Daily trend chart
    st.markdown('<div class="artifact-card">', unsafe_allow_html=True)
    st.subheader("📈 Daily Balance Trend")
    fig = px.line(
        df_f, x='date', y='balance',
        labels={'date':'Date','balance':'Balance'},
        markers=True
    )
    fig.update_traces(line=dict(color='#1f77b4', width=3),
                      marker=dict(size=6, line=dict(width=1, color='#1f77b4')))
    fig.update_layout(
        plot_bgcolor='white',
        xaxis=dict(showgrid=True, gridcolor='#ECECEC',
linecolor='#333', tickcolor='#333'),
        yaxis=dict(showgrid=True, gridcolor='#ECECEC',
linecolor='#333', tickcolor='#333')
    )
    st.plotly_chart(fig, use_container_width=True)
    st.markdown('</div>', unsafe_allow_html=True)

    if predict:
        # Forecast
        hist = df_f.set_index('date')['balance'].asfreq('D')
        model = ExponentialSmoothing(hist, trend='add', seasonal=None).fit()

        y, m = hist.index[-1].year, hist.index[-1].month
        last_day = monthrange(y, m)[1]
        future = pd.date_range(f"{y}-{m:02d}-25", f"{y}-{m:02d}-{last_day:02d}")
        fcast = model.forecast(len(future))
        fcast.index = future

        df_pred = pd.DataFrame({
            'date': future,
            'forecast': fcast.values.round(2),
            'confidence': 80,
            'idea': 'HW exponential smoothing'
        })

        # Forecast table
        st.markdown('<div class="artifact-card">', unsafe_allow_html=True)
        st.subheader("📋 Forecast Table")
        st.dataframe(df_pred, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

        # Forecast vs Actual
        st.markdown('<div class="artifact-card">', unsafe_allow_html=True)
        st.subheader("🔮 Forecast vs Actual")
        df_plot = (df_f.rename(columns={'balance':'actual'})
                   .merge(df_pred[['date','forecast']], on='date', how='right'))
        fig2 = px.line(df_plot, x='date', y=['actual','forecast'],
                       labels={'value':'Balance','variable':''}, markers=True)
        fig2.update_traces(selector=dict(name='actual'),
                           line=dict(color='#1f77b4', width=3))
        fig2.update_traces(selector=dict(name='forecast'),
                           line=dict(color='#ff7f0e', width=3, dash='dash'))
        fig2.update_layout(
            plot_bgcolor='white',
            xaxis=dict(showgrid=True, gridcolor='#ECECEC',
linecolor='#333', tickcolor='#333'),
            yaxis=dict(showgrid=True, gridcolor='#ECECEC',
linecolor='#333', tickcolor='#333')
        )
        st.plotly_chart(fig2, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

        # Confidence chart
        st.markdown('<div class="artifact-card">', unsafe_allow_html=True)
        st.subheader("📊 Confidence (%) Over Time")
        fig3 = px.bar(df_pred, x='date', y='confidence',
                      labels={'confidence':'Confidence (%)'})
        fig3.update_traces(marker_color='#1f77b4',
marker_line_color='#333', marker_line_width=1)
        fig3.update_layout(
            plot_bgcolor='white',
            xaxis=dict(linecolor='#333', tickcolor='#333'),
            yaxis=dict(linecolor='#333', tickcolor='#333')
        )
        st.plotly_chart(fig3, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

        # Trending Ideas
        st.markdown('<div class="artifact-card">', unsafe_allow_html=True)
        st.subheader("💡 Trending Ideas")
        for _, r in df_pred.iterrows():
            st.markdown(f"- **{r.date.date()}**: {r.idea} _(conf
{r.confidence}%)_")
        st.markdown('</div>', unsafe_allow_html=True)

        # Market indicators
        st.markdown('<div class="indicator-banner">', unsafe_allow_html=True)
        st.markdown('<div class="artifact-card">', unsafe_allow_html=True)
        st.subheader("📊 Market Indicators Used")
        fx_used = df_fx[(df_fx['date'] >= df_f['date'].min()) &
                        (df_fx['date'] <= df_f['date'].max())]
        st.dataframe(fx_used[['date','fx_volatility','interest_rate']],
use_container_width=True)
        fig_ind = px.line(fx_used, x='date',
                          y=['fx_volatility','interest_rate'],
                          labels={'value':'Indicator','variable':'Type'})
        fig_ind.update_traces(line=dict(width=2))
        fig_ind.update_layout(
            plot_bgcolor='white',
            xaxis=dict(linecolor='#333', tickcolor='#333'),
            yaxis=dict(linecolor='#333', tickcolor='#333')
        )
        st.plotly_chart(fig_ind, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

    else:
        st.info("Click **Forecast** to run the prediction.")
