# app.py
import streamlit as st
import pandas as pd
import plotly.express as px
from pipeline import run_etl_pipeline

st.set_page_config(page_title="Airbnb ETL Dashboard", layout="wide")
st.title(" Airbnb ETL & Interactive Dashboard")

# -------------------------------
# Session state initialization
# -------------------------------
if "df_transformed" not in st.session_state:
    st.session_state.df_transformed = None

# -------------------------------
# File uploader
# -------------------------------
uploaded_file = st.file_uploader("Choose a CSV file", type="csv")

if uploaded_file is not None:
    if "df_raw" not in st.session_state:
        st.session_state.df_raw = pd.read_csv(uploaded_file)
    df = st.session_state.df_raw
    st.write(f" Loaded {len(df)} rows from CSV.")
    
    # Run ETL only once
    if st.session_state.df_transformed is None:
        if st.button("Run ETL Pipeline"):
            st.session_state.df_transformed = run_etl_pipeline(df)
            st.success(" ETL Pipeline completed!")

    if st.session_state.df_transformed is not None:
        df_transformed = st.session_state.df_transformed
        
        # -------------------------------
        # Sidebar Filters
        # -------------------------------
        st.sidebar.header("Filter Data")
        neighbourhoods = st.sidebar.multiselect(
            "Neighbourhood Group",
            options=df_transformed['neighbourhood_group'].unique(),
            default=df_transformed['neighbourhood_group'].unique()
        )
        price_cats = st.sidebar.multiselect(
            "Price Category",
            options=df_transformed['price_category'].unique(),
            default=df_transformed['price_category'].unique()
        )
        min_price = int(df_transformed['price'].min())
        max_price = int(df_transformed['price'].max())
        price_range = st.sidebar.slider(
            "Price Range",
            min_value=min_price,
            max_value=max_price,
            value=(min_price, max_price),
            step=10
        )

        # Apply all filters
        filtered_df = df_transformed[
            (df_transformed['neighbourhood_group'].isin(neighbourhoods)) &
            (df_transformed['price_category'].isin(price_cats)) &
            (df_transformed['price'] >= price_range[0]) &
            (df_transformed['price'] <= price_range[1])
        ]

        # -------------------------------
        # Display data and charts
        # -------------------------------
        st.subheader("Transformed Data (First 10 rows)")
        st.dataframe(filtered_df.head(10))

        # Average price bar chart
        agg_df = filtered_df.groupby('neighbourhood_group').agg(
            avg_price=('price','mean'), listings_count=('id','count')
        ).reset_index()
        fig1 = px.bar(
            agg_df, x='neighbourhood_group', y='avg_price', color='neighbourhood_group',
            text='avg_price', title="Average Price per Neighbourhood Group"
        )
        st.plotly_chart(fig1, use_container_width=True)

        # Price category pie chart
        price_cat_df = filtered_df.groupby('price_category')['id'].count().reset_index().rename(columns={'id':'count'})
        fig2 = px.pie(price_cat_df, names='price_category', values='count', hole=0.4,
                      title="Listings Distribution by Price Category")
        st.plotly_chart(fig2, use_container_width=True)

        # Scatter plot
        scatter_df = filtered_df[filtered_df['minimum_nights'] < 365]
        fig3 = px.scatter(scatter_df, x='minimum_nights', y='price', color='neighbourhood_group',
                          title="Price vs Minimum Nights", hover_data=['price','minimum_nights'])
        st.plotly_chart(fig3, use_container_width=True)

        # Download CSV
        csv = filtered_df.to_csv(index=False).encode('utf-8')
        st.download_button(
            " Download Filtered & Cleaned Data as CSV",
            data=csv, file_name="filtered_airbnb.csv", mime="text/csv"
        )
