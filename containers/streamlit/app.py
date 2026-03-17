import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
import requests

# config page and connection
st.set_page_config(
    page_title="Energy Lakehouse Analytics",
    page_icon="⚡",
    layout="wide"
)

# MinIO connection config
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio123")
BUCKET_NAME = "energy-lake"

# cache load function
@st.cache_data(ttl=600)  
def load_data(table_name):
    """read parquet file from MinIO Gold Layer"""
    s3_path = f"s3://{BUCKET_NAME}/gold/{table_name}.parquet"
    
    storage_options = {
        "key": MINIO_ACCESS_KEY,
        "secret": MINIO_SECRET_KEY,
        "client_kwargs": {"endpoint_url": MINIO_ENDPOINT}
    }
    
    try:
        return pd.read_parquet(s3_path, storage_options=storage_options)
    except Exception as e:
        st.error(f"Failed to read data {table_name}: {e}")
        return pd.DataFrame()

# sidebar navigation
st.sidebar.title("Energy Analytics Dashboard")
page = st.sidebar.radio(
    "choose analysis dimension", 
    ["1. Green Performance", 
     "2. Financial Risk", 
     "3. Duck Curve",
     "4. Real-Time Monitor"]
)

st.sidebar.markdown("---")
st.sidebar.info(f"data source: MinIO Gold Layer\nEndpoint: {MINIO_ENDPOINT}")

# page 1: Green Performance
if page == "1. Green Performance":
    st.title("Green Performance")
    st.markdown("analyze the changes in grid stability (volatility, ramping) when **green penetration** increases.")

    # load data
    df = load_data("mrt_country_green_performance")
    
    if not df.empty:
        # filter
        col1, col2 = st.columns(2)
        with col1:
            selected_country = st.selectbox("select country", df['country_name'].unique())
        with col2:
            selected_year = st.selectbox("select year", sorted(df['year'].unique(), reverse=True))

        # filter data
        temp_df = df[
            (df['country_name'] == selected_country) & 
            (df['year'] == selected_year)
        ]

        # clean: check if key indicators are all empty
        # if volatility or residual load is NaN or None, this row data has bucket but no actual meaning
        filtered_df = temp_df.dropna(subset=['avg_price_volatility', 'avg_residual_load'], how='any').sort_values("bucket_sort_order")

        if not filtered_df.empty:
            # layout: left table right chart
            col_left, col_right = st.columns([1, 2])

            with col_left:
                st.subheader("sensitivity summary")
                # format display
                display_df = filtered_df[[
                    'penetration_bucket', 'avg_price_volatility', 
                    'avg_residual_load', 'total_extreme_ramps', 'hours_in_bucket'
                ]].copy()
                
                # style
                st.dataframe(
                    display_df, 
                    hide_index=True,
                    column_config={
                        "penetration_bucket": "green penetration interval",
                        "avg_price_volatility": st.column_config.NumberColumn("price volatility (Std)", format="%.2f"),
                        "avg_residual_load": st.column_config.NumberColumn("residual load (MW)", format="%d"),
                        "total_extreme_ramps": "extreme ramps",
                        "hours_in_bucket": "hours in bucket"
                    },
                    use_container_width=True
                )

            with col_right:
                st.subheader("penetration vs. volatility correlation")
                fig = px.scatter(
                    filtered_df,
                    x="penetration_bucket",
                    y="avg_price_volatility",
                    size="total_extreme_ramps",   # Size: extreme ramps
                    color="avg_residual_load",    # Color: residual load
                    hover_data=["hours_in_bucket", "avg_price"],
                    labels={
                        "penetration_bucket": "green penetration interval",
                        "avg_price_volatility": "average price volatility (€/MWh)",
                        "total_extreme_ramps": "extreme ramps",
                        "avg_residual_load": "average residual load (MW)"
                    },
                    color_continuous_scale="Viridis",
                    size_max=60
                )
                st.plotly_chart(fig, use_container_width=True)
        else:
            # no value
            st.warning(f"The region has generation records in {selected_year}, but lacks price or load data, unable to perform sensitivity analysis.")

# page 2: Financial Risk
elif page == "2. Financial Risk":
    st.title("Dunkelflaute Cost Analysis")
    st.markdown("In extreme weather conditions, the additional risk cost caused by price increases.")

    df = load_data("mrt_financial_risk")

    if not df.empty:
        # filter
        countries = df['country_name'].unique()
        selected_countries = st.multiselect("compare countries", countries, default=countries[:2] if len(countries)>1 else countries)
        
        filtered_df = df[df['country_name'].isin(selected_countries)]

        # critical metrics
        total_risk_eur = filtered_df['total_dunkelflaute_cost_eur'].sum()
        total_hours = filtered_df['hours_in_dunkelflaute'].sum()
        
        kpi1, kpi2, kpi3 = st.columns(3)
        kpi1.metric("cumulative risk premium", f"€{total_risk_eur/1e6:.2f} M")
        kpi2.metric("affected hours", f"{total_hours} Hours")
        if total_hours > 0:
            avg_premium = total_risk_eur / filtered_df['priced_load_mwh_at_risk'].sum()
            kpi3.metric("average premium level", f"€{avg_premium:.2f} / MWh")

        st.divider()

        # monthly cost distribution
        st.subheader("Monthly Risk Cost Distribution")
        
        # create a Year-Month field for better chart display
        filtered_df['date_str'] = filtered_df['year'].astype(str) + '-' + filtered_df['month'].astype(str).str.zfill(2)
        filtered_df = filtered_df.sort_values(['year', 'month'])

        fig = px.bar(
            filtered_df,
            x="date_str",
            y="total_dunkelflaute_cost_eur",
            color="country_name",
            title="Dunkelflaute Cost Over Time (Monthly)",
            labels={
                "date_str": "time (year-month)",
                "total_dunkelflaute_cost_eur": "risk premium (€)",
                "country_name": "country"
            },
            barmode='group'
        )
        st.plotly_chart(fig, use_container_width=True)

# Page 3: Duck Curve Analysis
elif page == "3. Duck Curve Analysis":
    st.title("Duck Curve Analysis")
    st.markdown("compare **total load** with **net load** to observe the impact of photovoltaic on the grid.")

    df = load_data("mrt_duck_curve_monthly")

    if not df.empty:
        col1, col2, col3 = st.columns(3)
        with col1:
            country = st.selectbox("select country", df['country_name'].unique(), key="duck_country")
        with col2:
            year = st.selectbox("select year", sorted(df['year'].unique(), reverse=True), key="duck_year")
        with col3:
            month = st.slider("select month", 1, 12, 6) # default to June

        # filter data
        curve_data = df[
            (df['country_name'] == country) & 
            (df['year'] == year) & 
            (df['month'] == month)
        ].sort_values("hour_of_day")

        if not curve_data.empty:
            st.subheader(f"{country} - Year {year} Month {month} Typical Duck Curve")
            
            # use Plotly Graph Objects to draw the duck curve
            fig = go.Figure()

            # Total Load - gray dashed line
            fig.add_trace(go.Scatter(
                x=curve_data['hour_of_day'], 
                y=curve_data['avg_total_load'],
                mode='lines',
                name='Total Load',
                line=dict(color='gray', dash='dash')
            ))

            # Net Load - core curve, red bold line
            fig.add_trace(go.Scatter(
                x=curve_data['hour_of_day'], 
                y=curve_data['avg_net_load'],
                mode='lines+markers',
                name='Net Load',
                line=dict(color='#FF4B4B', width=4),
                fill='tozeroy', # fill bottom
                fillcolor='rgba(255, 75, 75, 0.1)' 
            ))

            # Solar Generation - yellow area
            fig.add_trace(go.Scatter(
                x=curve_data['hour_of_day'], 
                y=curve_data['avg_solar_generation'],
                mode='lines',
                name='Solar Generation',
                line=dict(color='#FFD700'),
                stackgroup='one' # stack group, just for display auxiliary information
            ))

            fig.update_layout(
                xaxis_title="Hour (0-23)",
                yaxis_title="Power (MW)",
                hovermode="x unified",
                legend=dict(orientation="h", y=1.1)
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # explanatory text
            min_net_load = curve_data['avg_net_load'].min()
            max_ramp = (curve_data['avg_net_load'].diff()).max()
            
            st.info(f"""
            **Shape Interpretation**:
            - **belly (minimum point)**: net load decreases to **{min_net_load:.0f} MW**, this is the peak of photovoltaic generation.
            - **neck (maximum ramp)**: net load increases by **{max_ramp:.0f} MW** in one hour, requiring quick intervention.
            """)
        else:
            st.warning("No data for this month.")

# Page 4: Real-Time Market Monitor
elif page == "4. Real-Time Market Monitor":
    st.title("Real-Time Market Monitor")
    st.markdown("Compare **Awattar API Real-Time Day-Ahead Prices** with **Historical Normal Range (95% CI)** to identify price anomalies.")

    # auxiliary function
    @st.cache_data(ttl=300)
    def fetch_awattar_api(region="DE"):
        """Fetch real-time day-ahead prices from Awattar API."""
        urls = {
            "DE": "https://api.awattar.de/v1/marketdata",
            "AT": "https://api.awattar.at/v1/marketdata",
        }
        url = urls.get(region)
        if not url:
            return pd.DataFrame()
        try:
            resp = requests.get(url, timeout=15)
            if resp.status_code == 200:
                data = resp.json().get('data', [])
                if not data:
                    return pd.DataFrame()
                df = pd.DataFrame(data)
                df['event_time'] = pd.to_datetime(df['start_timestamp'], unit='ms')
                df['price_eur_mwh'] = df['marketprice']
                df['region'] = region
                df['hour'] = df['event_time'].dt.hour
                df['month'] = df['event_time'].dt.month
                return df[['event_time', 'price_eur_mwh', 'region', 'hour', 'month']]
            else:
                return pd.DataFrame()
        except Exception:
            return pd.DataFrame()

    @st.cache_data(ttl=600)
    def load_realtime_data():
        """Load real-time data from MinIO realtime layer."""
        s3_path = f"s3://{BUCKET_NAME}/realtime/market_monitor"
        storage_options = {
            "key": MINIO_ACCESS_KEY,
            "secret": MINIO_SECRET_KEY,
            "client_kwargs": {"endpoint_url": MINIO_ENDPOINT}
        }
        try:
            df = pd.read_parquet(s3_path, storage_options=storage_options)
            return df
        except Exception:
            return pd.DataFrame()

    @st.cache_data(ttl=600)
    def load_historical_baseline():
        """Load historical data from MinIO gold layer, calculate mean and standard deviation by month+hour, and apply correction factor."""
        s3_path = f"s3://{BUCKET_NAME}/gold/fct_energy_hourly.parquet"
        storage_options = {
            "key": MINIO_ACCESS_KEY,
            "secret": MINIO_SECRET_KEY,
            "client_kwargs": {"endpoint_url": MINIO_ENDPOINT}
        }
        
        # define correction factor
        SCALE_FACTOR = 3.0
        
        try:
            df = pd.read_parquet(s3_path, storage_options=storage_options)
            
            # basic data cleaning
            if 'event_time' in df.columns and 'price_eur_mwh' in df.columns:
                # convert time type
                df['event_time'] = pd.to_datetime(df['event_time'])
                df['hour'] = df['event_time'].dt.hour
                df['month'] = df['event_time'].dt.month
                
                # handle country column name inconsistency
                country_col = 'country_code' if 'country_code' in df.columns else 'country'
                
                # aggregation calculation
                baseline = df.groupby([country_col, 'month', 'hour']).agg(
                    hist_mean=('price_eur_mwh', 'mean'),
                    hist_std=('price_eur_mwh', 'std')
                ).reset_index()
                
                # apply correction factor
                baseline['hist_mean'] = baseline['hist_mean'] * SCALE_FACTOR
                baseline['hist_std'] = baseline['hist_std'] * SCALE_FACTOR
                
                # rename column
                baseline.rename(columns={country_col: 'country_code'}, inplace=True)
                
                return baseline
            
            return pd.DataFrame()
        except Exception as e:
            # print error in Streamlit interface
            print(f"Error loading baseline: {e}")
            return pd.DataFrame()

    def build_chart(api_df, baseline_df, region, country_code):
        """Build single region Plotly chart: price line + confidence interval band"""
        fig = go.Figure()

        if not baseline_df.empty:
            bl = baseline_df[baseline_df['country_code'] == country_code].copy()
            if not bl.empty:
                bl = bl.sort_values('hour')
                bl['ci_upper'] = bl['hist_mean'] + 1.96 * bl['hist_std']
                bl['ci_lower'] = bl['hist_mean'] - 1.96 * bl['hist_std']

                # if api_df has data, filter baseline to API corresponding month
                if not api_df.empty:
                    target_month = api_df['month'].iloc[0]
                    bl = bl[bl['month'] == target_month]
                    if bl.empty:
                        bl = baseline_df[baseline_df['country_code'] == country_code].copy()
                        bl = bl.groupby('hour').agg(
                            hist_mean=('hist_mean', 'mean'),
                            hist_std=('hist_std', 'mean')
                        ).reset_index()
                        bl['ci_upper'] = bl['hist_mean'] + 1.96 * bl['hist_std']
                        bl['ci_lower'] = bl['hist_mean'] - 1.96 * bl['hist_std']
                else:
                    bl = bl.groupby('hour').agg(
                        hist_mean=('hist_mean', 'mean'),
                        hist_std=('hist_std', 'mean')
                    ).reset_index()
                    bl['ci_upper'] = bl['hist_mean'] + 1.96 * bl['hist_std']
                    bl['ci_lower'] = bl['hist_mean'] - 1.96 * bl['hist_std']

                # confidence interval band
                fig.add_trace(go.Scatter(
                    x=bl['hour'], y=bl['ci_upper'],
                    mode='lines', line=dict(width=0),
                    showlegend=False, hoverinfo='skip'
                ))
                fig.add_trace(go.Scatter(
                    x=bl['hour'], y=bl['ci_lower'],
                    mode='lines', line=dict(width=0),
                    fill='tonexty',
                    fillcolor='rgba(68, 168, 140, 0.2)',
                    name='Historical Normal Range (95% CI)'
                ))
                # historical mean dashed line
                fig.add_trace(go.Scatter(
                    x=bl['hour'], y=bl['hist_mean'],
                    mode='lines',
                    line=dict(color='rgba(68, 168, 140, 0.6)', dash='dash'),
                    name='Historical Mean (with Inflation/Energy Crisis Correction)'
                ))

        if not api_df.empty:
            region_df = api_df[api_df['region'] == region].sort_values('hour')
            if not region_df.empty:
                fig.add_trace(go.Scatter(
                    x=region_df['hour'],
                    y=region_df['price_eur_mwh'],
                    mode='lines+markers',
                    line=dict(color='#FF4B4B', width=3),
                    marker=dict(size=6),
                    name='Day-Ahead Price (Predicted)'
                ))

        fig.update_layout(
            title=f"{region} ({country_code}) Day-Ahead Price",
            xaxis_title="Hour (0-23)",
            yaxis_title="€/MWh",
            hovermode="x unified",
            legend=dict(orientation="h", y=1.12),
            height=450
        )
        return fig

    # load data
    with st.spinner("Loading data..."):
        baseline_df = load_historical_baseline()
        realtime_df = load_realtime_data()

        # decide to use streaming data or API downgrade
        if not realtime_df.empty and len(realtime_df) > 0:
            st.success("Spark Streaming Realtime Data Loaded")    
            api_df_de = realtime_df[realtime_df['region'] == 'DE'].copy()
            api_df_at = realtime_df[realtime_df['region'] == 'AT'].copy()
            if 'event_time' in realtime_df.columns:
                api_df_de['hour'] = pd.to_datetime(api_df_de['event_time']).dt.hour
                api_df_de['month'] = pd.to_datetime(api_df_de['event_time']).dt.month
                api_df_de['price_eur_mwh'] = api_df_de.get('price_eur_mwh', api_df_de.get('expected_price', 0))
                api_df_at['hour'] = pd.to_datetime(api_df_at['event_time']).dt.hour
                api_df_at['month'] = pd.to_datetime(api_df_at['event_time']).dt.month
                api_df_at['price_eur_mwh'] = api_df_at.get('price_eur_mwh', api_df_at.get('expected_price', 0))
        else:
            st.info("Streaming data not available, switched to Awattar API direct mode")    
            api_df_de = fetch_awattar_api("DE")
            api_df_at = fetch_awattar_api("AT")

    # display DE and AT side by side
    col_de, col_at = st.columns(2)

    with col_de:
        st.subheader("Germany (DE-LU)")
        if not api_df_de.empty:
            fig_de = build_chart(api_df_de, baseline_df, "DE", "DE_LU")
            st.plotly_chart(fig_de, use_container_width=True)
        else:
            st.warning("DE data not available")

    with col_at:
        st.subheader("Austria (AT)")
        if not api_df_at.empty:
            fig_at = build_chart(api_df_at, baseline_df, "AT", "AT")
            st.plotly_chart(fig_at, use_container_width=True)
        else:
            st.warning("AT data not available")