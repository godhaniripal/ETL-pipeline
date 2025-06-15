import pandas as pd
import numpy as np
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine, text
import os
import sys
from pathlib import Path
from datetime import datetime, timedelta

# Ensure paths are set correctly
sys.path.append(str(Path(__file__).absolute().parent.parent))
from src.utils import get_db_connection_string
from src.extraction.extract import fetch_disease_sh_data, fetch_covid19api_data, load_csv_data

# Set page configuration
st.set_page_config(
    page_title="COVID-19 & Vaccination Tracker",
    page_icon="🦠",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Sidebar title
st.sidebar.title('COVID-19 & Vaccination Tracker')

# Connect to database function
@st.cache_resource
def get_database_connection():
    """Create a database connection that can be reused"""
    conn_string = get_db_connection_string()
    engine = create_engine(conn_string)
    return engine

# Load data functions
@st.cache_data(ttl=3600)
def load_country_list():
    """Load the list of countries from the database"""
    try:
        engine = get_database_connection()
        query = text("""
            SELECT c.country_name, c.continent, c.population 
            FROM countries c
            ORDER BY c.country_name
        """)
        return pd.read_sql(query, engine)
    except Exception as e:
        st.error(f"Error loading country list: {str(e)}")
        return pd.DataFrame(columns=['country_name', 'continent', 'population'])

@st.cache_data(ttl=3600)
def load_latest_global_stats():
    """Load the latest global statistics"""
    try:
        engine = get_database_connection()
        query = text("""
            WITH latest_date AS (
                SELECT MAX(date) as max_date FROM covid_cases
            )
            SELECT 
                SUM(cc.total_cases) as total_cases,
                SUM(cc.total_deaths) as total_deaths,
                SUM(cc.active_cases) as active_cases,
                SUM(cc.total_recovered) as total_recovered,
                MAX(cc.date) as data_date
            FROM covid_cases cc, latest_date
            WHERE cc.date = latest_date.max_date
        """)
        return pd.read_sql(query, engine)
    except Exception as e:
        st.error(f"Error loading global stats: {str(e)}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def load_country_data(country_name=None, days=30):
    """Load data for specific country or all countries"""
    try:
        engine = get_database_connection()
        
        if country_name:
            # Query for specific country
            query = text(f"""
                SELECT 
                    c.country_name, 
                    cc.date, 
                    cc.total_cases, 
                    cc.new_cases,
                    cc.total_deaths, 
                    cc.new_deaths,
                    cc.active_cases,
                    cc.new_cases_7day_avg,
                    cc.new_deaths_7day_avg,
                    cc.case_fatality_rate,
                    v.total_vaccinations,
                    v.daily_vaccinations,
                    v.vaccination_rate
                FROM covid_cases cc
                JOIN countries c ON cc.country_id = c.country_id
                LEFT JOIN vaccinations v ON cc.country_id = v.country_id AND cc.date = v.date
                WHERE c.country_name = :country
                AND cc.date >= (SELECT MAX(date) - INTERVAL '{days} days' FROM covid_cases)
                ORDER BY cc.date
            """)
            return pd.read_sql(query, engine, params={"country": country_name})
        else:
            # Query for all countries (latest data)
            query = text("""
                WITH latest_date AS (
                    SELECT MAX(date) as max_date FROM covid_cases
                )
                SELECT 
                    c.country_name,
                    c.continent, 
                    cc.date, 
                    cc.total_cases, 
                    cc.new_cases,
                    cc.total_deaths, 
                    cc.new_deaths,
                    cc.active_cases,
                    cc.case_fatality_rate,
                    cc.cases_per_million,
                    cc.deaths_per_million,
                    v.total_vaccinations,
                    v.vaccination_rate
                FROM covid_cases cc
                JOIN countries c ON cc.country_id = c.country_id
                LEFT JOIN vaccinations v ON cc.country_id = v.country_id AND cc.date = v.date
                WHERE cc.date = (SELECT max_date FROM latest_date)
                ORDER BY cc.total_cases DESC
            """)
            return pd.read_sql(query, engine)
    except Exception as e:
        st.error(f"Error loading country data: {str(e)}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def load_time_series_data(metric='total_cases', days=90):
    """Load time series data for top countries"""
    try:
        engine = get_database_connection()
        query = text(f"""
            WITH top_countries AS (
                SELECT 
                    c.country_id,
                    c.country_name
                FROM covid_cases cc
                JOIN countries c ON cc.country_id = c.country_id
                WHERE cc.date = (SELECT MAX(date) FROM covid_cases)
                ORDER BY cc.{metric} DESC
                LIMIT 10
            )
            SELECT 
                c.country_name, 
                cc.date, 
                cc.{metric}
            FROM covid_cases cc
            JOIN top_countries c ON cc.country_id = c.country_id
            WHERE cc.date >= (SELECT MAX(date) - INTERVAL '{days} days' FROM covid_cases)
            ORDER BY cc.date, cc.{metric} DESC
        """)
        return pd.read_sql(query, engine)
    except Exception as e:
        st.error(f"Error loading time series data: {str(e)}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def load_vaccination_data(top_n=20):
    """Load vaccination data for top countries"""
    try:
        engine = get_database_connection()
        query = text(f"""
            WITH latest_date AS (
                SELECT MAX(date) as max_date FROM vaccinations
            )
            SELECT 
                c.country_name,
                c.population,
                v.total_vaccinations,
                v.vaccination_rate
            FROM vaccinations v
            JOIN countries c ON v.country_id = c.country_id
            WHERE v.date = (SELECT max_date FROM latest_date)
            AND c.population IS NOT NULL
            AND v.total_vaccinations IS NOT NULL
            ORDER BY v.vaccination_rate DESC
            LIMIT {top_n}
        """)
        return pd.read_sql(query, engine)
    except Exception as e:
        st.error(f"Error loading vaccination data: {str(e)}")
        return pd.DataFrame()

# Functions to upload manual data
def upload_csv_data():
    uploaded_file = st.sidebar.file_uploader("Upload CSV Data", type=['csv'])
    if uploaded_file is not None:
        try:
            # Save the file to a temporary location
            temp_file = Path("./data/raw") / f"temp_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            os.makedirs(os.path.dirname(temp_file), exist_ok=True)
            
            with open(temp_file, "wb") as f:
                f.write(uploaded_file.getbuffer())
            
            st.sidebar.success(f"File saved as {temp_file}")
            
            # Allow user to process the file
            if st.sidebar.button("Process Uploaded File"):
                # Import the ETL pipeline
                from etl_pipeline import run_etl_pipeline
                success = run_etl_pipeline(str(temp_file))
                
                if success:
                    st.sidebar.success("Data processed and loaded successfully!")
                    # Set a flag to trigger page refresh
                    st.session_state.data_uploaded = True
                else:
                    st.sidebar.error("Error processing data. Check the logs.")
        
        except Exception as e:
            st.sidebar.error(f"Error handling uploaded file: {str(e)}")

# Function to run ETL pipeline
def run_etl():
    if st.sidebar.button("Refresh Data from APIs"):
        try:
            from etl_pipeline import run_etl_pipeline
            with st.spinner('Fetching and processing data...'):
                success = run_etl_pipeline()
                
                if success:
                    st.sidebar.success("Data refreshed successfully!")
                    # Set a flag to trigger page refresh
                    st.session_state.data_refreshed = True
                else:
                    st.sidebar.error("Error refreshing data. Check the logs.")
        
        except Exception as e:
            st.sidebar.error(f"Error running ETL pipeline: {str(e)}")

# Visualization functions
def plot_global_overview():
    """Plot global overview metrics"""
    try:
        global_stats = load_latest_global_stats()
        
        if not global_stats.empty:
            # Create metrics in a row
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Total Cases", f"{global_stats['total_cases'].iloc[0]:,}")
            
            with col2:
                st.metric("Total Deaths", f"{global_stats['total_deaths'].iloc[0]:,}")
            
            with col3:
                st.metric("Active Cases", f"{global_stats['active_cases'].iloc[0]:,}")
            
            with col4:
                st.metric("Recovered", f"{global_stats['total_recovered'].iloc[0]:,}")
            
            st.caption(f"Data as of: {global_stats['data_date'].iloc[0]}")
        else:
            st.warning("No global statistics available")
    
    except Exception as e:
        st.error(f"Error displaying global overview: {str(e)}")

def plot_country_comparison(metric='total_cases', top_n=10):
    """Plot country comparison for selected metric"""
    try:
        all_countries = load_country_data()
        
        if not all_countries.empty:
            # Filter for top countries
            top_countries = all_countries.nlargest(top_n, metric)
            
            # Create bar chart
            fig = px.bar(
                top_countries,
                x='country_name',
                y=metric,
                color='continent',
                title=f"Top {top_n} Countries by {metric.replace('_', ' ').title()}",
                labels={'country_name': 'Country', metric: metric.replace('_', ' ').title()},
                template='plotly_white'
            )
            
            fig.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("No country comparison data available")
    
    except Exception as e:
        st.error(f"Error displaying country comparison: {str(e)}")

def plot_time_series(metric='total_cases', days=90):
    """Plot time series for top countries"""
    try:
        time_series = load_time_series_data(metric, days)
        
        if not time_series.empty:
            # Create line chart
            fig = px.line(
                time_series,
                x='date',
                y=metric,
                color='country_name',
                title=f"{metric.replace('_', ' ').title()} Over Time (Last {days} Days)",
                labels={'date': 'Date', metric: metric.replace('_', ' ').title(), 'country_name': 'Country'},
                template='plotly_white'
            )
            
            fig.update_layout(legend_title_text='Country')
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("No time series data available")
    
    except Exception as e:
        st.error(f"Error displaying time series: {str(e)}")

def plot_country_detail(country_name, days=30):
    """Plot detailed analysis for a specific country"""
    try:
        country_data = load_country_data(country_name, days)
        
        if not country_data.empty:
            # Display metrics
            latest = country_data.iloc[-1]
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Total Cases", f"{latest['total_cases']:,}")
            
            with col2:
                st.metric("Total Deaths", f"{latest['total_deaths']:,}")
            
            with col3:
                st.metric("Active Cases", f"{latest['active_cases']:,}")
            
            with col4:
                if pd.notna(latest.get('vaccination_rate')):
                    st.metric("Vaccination Rate", f"{latest['vaccination_rate']:.2f}%")
                else:
                    st.metric("Vaccination Rate", "N/A")
            
            # Create tabs for different visualizations
            tab1, tab2, tab3 = st.tabs(["Cases & Deaths", "Vaccination", "Moving Averages"])
            
            with tab1:
                # Daily cases and deaths over time
                fig = go.Figure()
                
                # Add cases as bars
                fig.add_trace(go.Bar(
                    x=country_data['date'],
                    y=country_data['new_cases'],
                    name='New Cases',
                    marker_color='lightblue'
                ))
                
                # Add deaths as lines
                fig.add_trace(go.Scatter(
                    x=country_data['date'],
                    y=country_data['new_deaths'],
                    mode='lines',
                    name='New Deaths',
                    line=dict(color='red', width=2)
                ))
                
                # Update layout
                fig.update_layout(
                    title=f"Daily New Cases and Deaths in {country_name} (Last {days} Days)",
                    xaxis_title="Date",
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                    template='plotly_white'
                )
                
                st.plotly_chart(fig, use_container_width=True)
            
            with tab2:
                # Check if vaccination data is available
                if 'total_vaccinations' in country_data.columns and country_data['total_vaccinations'].notna().any():
                    # Vaccination progress
                    fig = go.Figure()
                    
                    # Add total vaccinations
                    fig.add_trace(go.Scatter(
                        x=country_data['date'],
                        y=country_data['total_vaccinations'],
                        mode='lines',
                        name='Total Vaccinations',
                        line=dict(color='green', width=3)
                    ))
                    
                    # Add daily vaccinations as bars if available
                    if 'daily_vaccinations' in country_data.columns and country_data['daily_vaccinations'].notna().any():
                        fig.add_trace(go.Bar(
                            x=country_data['date'],
                            y=country_data['daily_vaccinations'],
                            name='Daily Vaccinations',
                            marker_color='lightgreen'
                        ))
                    
                    # Update layout
                    fig.update_layout(
                        title=f"Vaccination Progress in {country_name}",
                        xaxis_title="Date",
                        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                        template='plotly_white'
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("No vaccination data available for this country")
            
            with tab3:
                # Moving averages
                if 'new_cases_7day_avg' in country_data.columns and country_data['new_cases_7day_avg'].notna().any():
                    fig = go.Figure()
                    
                    # Add daily new cases
                    fig.add_trace(go.Bar(
                        x=country_data['date'],
                        y=country_data['new_cases'],
                        name='Daily New Cases',
                        marker_color='lightblue',
                        opacity=0.7
                    ))
                    
                    # Add 7-day moving average
                    fig.add_trace(go.Scatter(
                        x=country_data['date'],
                        y=country_data['new_cases_7day_avg'],
                        mode='lines',
                        name='7-Day Moving Avg',
                        line=dict(color='blue', width=2)
                    ))
                    
                    # Update layout
                    fig.update_layout(
                        title=f"Daily New Cases and 7-Day Moving Average in {country_name}",
                        xaxis_title="Date",
                        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                        template='plotly_white'
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("No moving average data available for this country")
        else:
            st.warning(f"No data available for {country_name}")
    
    except Exception as e:
        st.error(f"Error displaying country detail: {str(e)}")

def plot_vaccination_comparison(top_n=20):
    """Plot vaccination rate comparison"""
    try:
        vax_data = load_vaccination_data(top_n)
        
        if not vax_data.empty:
            # Create horizontal bar chart
            fig = px.bar(
                vax_data,
                y='country_name',
                x='vaccination_rate',
                orientation='h',
                title=f"Top {top_n} Countries by Vaccination Rate",
                labels={'country_name': 'Country', 'vaccination_rate': 'Vaccination Rate (%)'},
                template='plotly_white',
                color='vaccination_rate',
                color_continuous_scale='Greens',
            )
            
            fig.update_layout(yaxis={'categoryorder': 'total ascending'})
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("No vaccination comparison data available")
    
    except Exception as e:
        st.error(f"Error displaying vaccination comparison: {str(e)}")

def create_heatmap():
    """Create a heatmap for case distribution by continent"""
    try:
        all_countries = load_country_data()
        
        if not all_countries.empty:
            # Prepare data for heatmap
            continent_pivot = all_countries.pivot_table(
                values='total_cases',
                index='continent',
                aggfunc=['sum', 'mean', 'count']
            ).reset_index()
            
            # Flatten the MultiIndex columns
            continent_pivot.columns = ['continent', 'total_cases', 'avg_cases', 'country_count']
            
            # Create heatmap with matplotlib/seaborn
            fig, ax = plt.subplots(figsize=(10, 6))
            
            # Create heatmap matrix
            heatmap_data = all_countries.pivot_table(
                values='cases_per_million',
                index='continent',
                columns=pd.qcut(all_countries['total_cases'], 4, duplicates='drop'),
                aggfunc='mean'
            )
            
            # Plot
            sns.heatmap(
                heatmap_data, 
                cmap='YlOrRd', 
                annot=True, 
                fmt='.0f',
                linewidths=0.5,
                ax=ax
            )
            
            plt.title('Cases Per Million by Continent and Case Quartile')
            plt.xlabel('Total Cases Quartile')
            plt.ylabel('Continent')
            plt.tight_layout()
            
            st.pyplot(fig)
        else:
            st.warning("No data available for heatmap")
    
    except Exception as e:
        st.error(f"Error creating heatmap: {str(e)}")

def create_choropleth(metric='total_cases'):
    """Create a global choropleth map"""
    try:
        all_countries = load_country_data()
        
        if not all_countries.empty:
            # Create choropleth map
            fig = px.choropleth(
                all_countries,
                locations='country_name',
                locationmode='country names',
                color=metric,
                hover_name='country_name',
                projection='natural earth',
                title=f'Global {metric.replace("_", " ").title()} Distribution',
                color_continuous_scale=px.colors.sequential.Plasma,
                labels={metric: metric.replace('_', ' ').title()}
            )
            
            fig.update_layout(coloraxis_colorbar=dict(title=metric.replace('_', ' ').title()))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("No data available for map")
    
    except Exception as e:
        st.error(f"Error creating choropleth map: {str(e)}")

# Main application
def main():
    # Check for page refresh flags
    if 'data_uploaded' in st.session_state and st.session_state.data_uploaded:
        st.session_state.data_uploaded = False
        st.rerun()
    
    if 'data_refreshed' in st.session_state and st.session_state.data_refreshed:
        st.session_state.data_refreshed = False
        st.rerun()
    
    # Sidebar options
    st.sidebar.header("Data Sources")
    
    # Option to refresh from APIs
    run_etl()
    
    # Option to upload CSV data
    st.sidebar.header("Upload Data")
    upload_csv_data()
    
    # Analysis options
    st.sidebar.header("Analysis Options")
    
    # Load country list for selection
    countries_df = load_country_list()
    country_names = sorted(countries_df['country_name'].tolist()) if not countries_df.empty else []
    
    view_option = st.sidebar.selectbox(
        "Select View",
        ["Global Overview", "Country Comparison", "Country Detail", "Vaccination Analysis"]
    )
    
    # Main content based on selected view
    st.title("COVID-19 & Vaccination Tracker")
    
    if view_option == "Global Overview":
        st.header("Global Overview")
        plot_global_overview()
        
        col1, col2 = st.columns(2)
        
        with col1:
            metric = st.selectbox(
                "Select Metric for Country Comparison",
                ["total_cases", "total_deaths", "active_cases", "case_fatality_rate", "cases_per_million"]
            )
            plot_country_comparison(metric)
        
        with col2:
            time_metric = st.selectbox(
                "Select Metric for Time Series",
                ["total_cases", "total_deaths", "active_cases"]
            )
            plot_time_series(time_metric)
        
        # Add global map
        st.header("Global Distribution")
        map_metric = st.selectbox(
            "Select Metric for Map",
            ["total_cases", "total_deaths", "cases_per_million", "deaths_per_million"]
        )
        create_choropleth(map_metric)
    
    elif view_option == "Country Comparison":
        st.header("Country Comparison")
        
        col1, col2 = st.columns([1, 1])
        
        with col1:
            metric = st.selectbox(
                "Select Metric",
                ["total_cases", "total_deaths", "active_cases", "new_cases", 
                 "case_fatality_rate", "cases_per_million", "deaths_per_million"]
            )
        
        with col2:
            top_n = st.slider("Number of Countries", min_value=5, max_value=30, value=10)
        
        plot_country_comparison(metric, top_n)
        
        # Add heatmap for additional analysis
        st.header("Continent Analysis")
        create_heatmap()
    
    elif view_option == "Country Detail":
        st.header("Country Detail")
        
        col1, col2 = st.columns([3, 1])
        
        with col1:
            selected_country = st.selectbox("Select Country", country_names)
        
        with col2:
            days = st.selectbox("Time Period", [30, 60, 90, 180], index=0)
        
        if selected_country:
            plot_country_detail(selected_country, days)
    
    elif view_option == "Vaccination Analysis":
        st.header("Vaccination Analysis")
        
        top_n = st.slider("Number of Countries", min_value=5, max_value=50, value=20)
        plot_vaccination_comparison(top_n)
        
        # Add vaccination vs cases scatter plot
        try:
            all_countries = load_country_data()
            vax_data = load_vaccination_data(200)  # Get more countries for better comparison
            
            if not all_countries.empty and not vax_data.empty:
                # Merge datasets
                merged = all_countries.merge(vax_data, on='country_name', how='inner')
                
                # Create scatter plot
                fig = px.scatter(
                    merged,
                    x='vaccination_rate',
                    y='case_fatality_rate',
                    size='total_cases',
                    color='continent',
                    hover_name='country_name',
                    title='Vaccination Rate vs. Case Fatality Rate',
                    labels={
                        'vaccination_rate': 'Vaccination Rate (%)',
                        'case_fatality_rate': 'Case Fatality Rate (%)'
                    },
                    template='plotly_white',
                    log_y=st.checkbox("Log Scale for Y-axis", value=False)
                )
                
                fig.update_layout(height=600)
                st.plotly_chart(fig, use_container_width=True)
        
        except Exception as e:
            st.error(f"Error creating vaccination scatter plot: {str(e)}")

if __name__ == "__main__":
    main()
