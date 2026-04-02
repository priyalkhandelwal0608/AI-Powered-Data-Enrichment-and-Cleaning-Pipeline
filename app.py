# app.py
import streamlit as st
import pandas as pd
import plotly.express as px
from pipeline import run_etl_pipeline

st.set_page_config(page_title="Airbnb ETL Dashboard", layout="wide")
st.title("🏠 Airbnb ETL Dashboard (Apache Beam + Visualization)")

# -------------------------------
# Session State
# -------------------------------
if "df_transformed" not in st.session_state:
    st.session_state.df_transformed = None

if "file_saved" not in st.session_state:
    st.session_state.file_saved = False

# -------------------------------
# Upload File
# -------------------------------
uploaded_file = st.file_uploader("Upload Airbnb CSV", type="csv")

if uploaded_file is not None:

    # Save file once
    if not st.session_state.file_saved:
        with open("temp.csv", "wb") as f:
            f.write(uploaded_file.getbuffer())
        st.session_state.file_saved = True

    st.success("✅ File uploaded successfully!")

    # -------------------------------
    # Run ETL
    # -------------------------------
    if st.session_state.df_transformed is None:
        if st.button("🚀 Run ETL Pipeline"):
            with st.spinner("Running Apache Beam Pipeline..."):
                st.session_state.df_transformed = run_etl_pipeline("temp.csv")
            st.success("✅ ETL Completed!")

    # -------------------------------
    # After ETL
    # -------------------------------
    if st.session_state.df_transformed is not None:
        df = st.session_state.df_transformed

        if df.empty:
            st.error("❌ No data processed.")
            st.stop()

        # -------------------------------
        # KPI CARDS
        # -------------------------------
        col1, col2, col3 = st.columns(3)

        col1.metric("Total Listings", len(df))
        col2.metric("Average Price", f"${int(df['price'].mean())}")
        col3.metric("Max Price", f"${int(df['price'].max())}")

        st.markdown("---")

        # -------------------------------
        # Sidebar Filters
        # -------------------------------
        st.sidebar.header("🔍 Filters")

        neighbourhoods = st.sidebar.multiselect(
            "Neighbourhood",
            options=sorted(df['neighbourhood_group'].dropna().unique()),
            default=sorted(df['neighbourhood_group'].dropna().unique())
        )

        price_cats = st.sidebar.multiselect(
            "Price Category",
            options=sorted(df['price_category'].dropna().unique()),
            default=sorted(df['price_category'].dropna().unique())
        )

        min_price = int(df['price'].min())
        max_price = int(df['price'].max())

        price_range = st.sidebar.slider(
            "Price Range",
            min_value=min_price,
            max_value=max_price,
            value=(min_price, max_price)
        )

        # Apply filters
        filtered_df = df[
            (df['neighbourhood_group'].isin(neighbourhoods)) &
            (df['price_category'].isin(price_cats)) &
            (df['price'] >= price_range[0]) &
            (df['price'] <= price_range[1])
        ]

        # -------------------------------
        # MAP VISUALIZATION 🔥
        # -------------------------------
        st.subheader("🗺️ Listings Map")

        if 'latitude' in filtered_df.columns and 'longitude' in filtered_df.columns:
            map_df = filtered_df[['latitude', 'longitude', 'price']].dropna()

            fig_map = px.scatter_mapbox(
                map_df,
                lat="latitude",
                lon="longitude",
                size="price",
                zoom=10,
                height=400
            )

            fig_map.update_layout(mapbox_style="open-street-map")
            st.plotly_chart(fig_map, use_container_width=True)
        else:
            st.warning("⚠️ Map data not available")

        # -------------------------------
        # DATA PREVIEW
        # -------------------------------
        st.subheader("📄 Data Preview")
        st.dataframe(filtered_df.head(10))

        # -------------------------------
        # BAR CHART
        # -------------------------------
        st.subheader("📊 Avg Price by Neighbourhood")

        agg_df = filtered_df.groupby('neighbourhood_group').agg(
            avg_price=('price', 'mean')
        ).reset_index()

        fig1 = px.bar(agg_df, x='neighbourhood_group', y='avg_price', color='neighbourhood_group')
        st.plotly_chart(fig1, use_container_width=True)

        # -------------------------------
        # PIE CHART
        # -------------------------------
        st.subheader("🥧 Price Category Distribution")

        pie_df = filtered_df.groupby('price_category').size().reset_index(name='count')
        fig2 = px.pie(pie_df, names='price_category', values='count', hole=0.4)
        st.plotly_chart(fig2, use_container_width=True)

        # -------------------------------
        # SCATTER
        # -------------------------------
        st.subheader("📈 Price vs Minimum Nights")

        if 'minimum_nights' in filtered_df.columns:
            fig3 = px.scatter(
                filtered_df,
                x='minimum_nights',
                y='price',
                color='neighbourhood_group'
            )
            st.plotly_chart(fig3, use_container_width=True)

        # -------------------------------
        # DOWNLOAD
        # -------------------------------
        csv = filtered_df.to_csv(index=False).encode('utf-8')

        st.download_button(
            "📥 Download Data",
            data=csv,
            file_name="filtered_data.csv",
            mime="text/csv"
        )
