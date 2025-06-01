import streamlit as st
import pandas as pd
import sqlalchemy as sa
import plotly.express as px
import plotly.graph_objects as go
# ======================
# CONFIG & CONNECTION
# ======================
@st.cache_resource
def get_engine():
    """
    Khởi tạo SQLAlchemy engine từ Streamlit Secrets.
    """
    db_url = st.secrets["database"]["url"]
    return sa.create_engine(db_url)

# ======================
# DATA FETCH FUNCTIONS
# ======================
@st.cache_data(show_spinner=False)
def fetch_sales(year_from, year_to, markets, categories):
    engine = get_engine()
    conn = engine.raw_connection()

    params = [year_from, year_to]
    where = ["d.year BETWEEN ? AND ?"]
    if markets:
        ph = ",".join("?" for _ in markets)
        where.append(f"m.market IN ({ph})")
        params += markets
    if categories:
        ph = ",".join("?" for _ in categories)
        where.append(f"c.category_name IN ({ph})")
        params += categories

    sql = f"""
    SELECT
      d.year,
      d.month,
      SUM(f.sales_amount)      AS sales,
      SUM(f.benefit_per_order) AS profit
    FROM DONHANG.dbo.Fact_Sales AS f
    JOIN DONHANG.dbo.Dim_Date     AS d ON f.order_date   = d.date_key
    JOIN DONHANG.dbo.Dim_Market   AS m ON f.market       = m.market
    JOIN DONHANG.dbo.Dim_Category AS c ON f.category_id  = c.category_id
    WHERE {' AND '.join(where)}
    GROUP BY d.year, d.month
    ORDER BY d.year, d.month;
    """
    df = pd.read_sql(sql, conn, params=params)
    conn.close()
    return df

@st.cache_data(show_spinner=False)
def fetch_market_sales(year_from, year_to, markets):
    engine = get_engine()
    conn = engine.raw_connection()

    params = [year_from, year_to]
    where = ["d.year BETWEEN ? AND ?"]
    if markets:
        ph = ",".join("?" for _ in markets)
        where.append(f"m.market IN ({ph})")
        params += markets

    sql = f"""
    SELECT
      m.market,
      SUM(f.sales_amount) AS sales
    FROM DONHANG.dbo.Fact_Sales AS f
    JOIN DONHANG.dbo.Dim_Date   AS d ON f.order_date = d.date_key
    JOIN DONHANG.dbo.Dim_Market AS m ON f.market     = m.market
    WHERE {' AND '.join(where)}
    GROUP BY m.market
    ORDER BY sales DESC;
    """
    df = pd.read_sql(sql, conn, params=params)
    conn.close()
    return df

# ----- Các hàm mới -----
@st.cache_data(show_spinner=False)
def fetch_category_sales(year_from, year_to, markets, categories):
    engine = get_engine()
    conn = engine.raw_connection()

    params = [year_from, year_to]
    where = ["d.year BETWEEN ? AND ?"]
    if markets:
        ph = ",".join("?" for _ in markets)
        where.append(f"m.market IN ({ph})")
        params += markets
    if categories:
        ph = ",".join("?" for _ in categories)
        where.append(f"c.category_name IN ({ph})")
        params += categories

    sql = f"""
    SELECT
      c.category_name,
      SUM(f.sales_amount)      AS sales,
      SUM(f.benefit_per_order) AS profit
    FROM DONHANG.dbo.Fact_Sales AS f
    JOIN DONHANG.dbo.Dim_Date     AS d ON f.order_date   = d.date_key
    JOIN DONHANG.dbo.Dim_Market   AS m ON f.market       = m.market
    JOIN DONHANG.dbo.Dim_Category AS c ON f.category_id  = c.category_id
    WHERE {' AND '.join(where)}
    GROUP BY c.category_name
    ORDER BY sales DESC;
    """
    df = pd.read_sql(sql, conn, params=params)
    conn.close()
    return df

@st.cache_data(show_spinner=False)
def fetch_top_products(year_from, year_to, markets, categories, top_n=10):
    engine = get_engine()
    conn = engine.raw_connection()

    params = [year_from, year_to]
    where = ["d.year BETWEEN ? AND ?"]
    if markets:
        ph = ",".join("?" for _ in markets)
        where.append(f"m.market IN ({ph})")
        params += markets
    if categories:
        ph = ",".join("?" for _ in categories)
        where.append(f"c.category_name IN ({ph})")
        params += categories

    sql = f"""
    SELECT TOP {top_n}
      p.product_name,
      SUM(f.sales_amount) AS sales_amount
    FROM DONHANG.dbo.Fact_Sales AS f
    JOIN DONHANG.dbo.Dim_Date     AS d ON f.order_date    = d.date_key
    JOIN DONHANG.dbo.Dim_Market   AS m ON f.market        = m.market
    JOIN DONHANG.dbo.Dim_Category AS c ON f.category_id   = c.category_id
    JOIN DONHANG.dbo.Dim_Product  AS p ON f.product_card_id = p.product_card_id
    WHERE {' AND '.join(where)}
    GROUP BY p.product_name
    ORDER BY sales_amount DESC;
    """
    df = pd.read_sql(sql, conn, params=params)
    conn.close()
    return df

@st.cache_data(show_spinner=False)
def fetch_shipping(year_from, year_to, regions, modes):
    engine = get_engine()
    conn = engine.raw_connection()

    params = [year_from, year_to]
    where = ["d.year BETWEEN ? AND ?"]
    if regions:
        ph = ",".join("?" for _ in regions)
        where.append(f"l.region IN ({ph})")
        params += regions
    if modes:
        ph = ",".join("?" for _ in modes)
        where.append(f"s.shipping_mode IN ({ph})")
        params += modes

    sql = f"""
    SELECT
      s.order_id,
      s.shipping_date,
      d.month,
      l.city,
      s.shipping_mode,
      s.days_for_shipment_scheduled,
      s.days_for_shipping_real,
      CASE
        WHEN s.days_for_shipping_real > s.days_for_shipment_scheduled THEN 1 ELSE 0
      END AS is_late
    FROM DONHANG.dbo.Fact_Shipping AS s
    JOIN DONHANG.dbo.Dim_Date          AS d ON s.shipping_date = d.date_key
    JOIN DONHANG.dbo.Dim_Location      AS l ON s.location_id    = l.location_id
    JOIN DONHANG.dbo.Dim_Shipping_Mode AS m ON s.shipping_mode   = m.shipping_mode
    WHERE {' AND '.join(where)}
    """
    df = pd.read_sql(sql, conn, params=params)
    conn.close()
    return df

# ======================
# DASHBOARD FUNCTIONS
# ======================
def show_sales_profit_dashboard():
    st.header("📈 Doanh thu & Lợi nhuận")
    year_from, year_to = st.sidebar.slider("Khoảng năm", 2015, 2018, (2015, 2018))
    all_markets    = ["Europe", "LATAM", "USCA", "Africa", "Pacific Asia"]
    all_categories = ["Fishing", "Camping & Outdoors", "Cardio Equipment", "Discs Shop", "Computers"]
    markets        = st.sidebar.multiselect("Chọn Market", all_markets, default=all_markets[:2])
    categories     = st.sidebar.multiselect("Chọn Category", all_categories, default=all_categories[:2])

    # gọi lại đúng các fetch
    df      = fetch_sales(year_from, year_to, markets, categories)
    df_mkt  = fetch_market_sales(year_from, year_to, markets)
    df_cat  = fetch_category_sales(year_from, year_to, markets, categories)
    df_top  = fetch_top_products(year_from, year_to, markets, categories, top_n=10)

    # KPI row
    total_sales  = df['sales'].sum()
    total_profit = df['profit'].sum()
    gp_pct       = total_profit / total_sales if total_sales else 0
    c1, c2, c3 = st.columns([1,1,2])
    c1.metric("Tổng Doanh Thu", f"{total_sales/1e6:.2f}M")
    c2.metric("Tổng Lợi Nhuận", f"{total_profit/1e6:.2f}M")

    # bullet gauge (quarter hiện tại)
    # bullet gauge (quarter hiện tại, hiển thị % so với goal)
    df['quarter'] = ((df.month - 1)//3 + 1)
    df_q = df.groupby('quarter', as_index=False).sum()
    last_q = df_q.iloc[-1]
    goal   = df_q['sales'].mean()

    # tỷ lệ (%) doanh thu quý hiện tại so với mục tiêu
    pct = (last_q['sales'] / goal) * 100 if goal else 0

    fig_g = go.Figure(go.Indicator(
        mode="number+gauge",
        value=pct,
        number={
            'suffix': '%',
            'font': {'size': 24}
        },
        title={
            'text': f"Quý {int(last_q['quarter'])}",
            'font': {'size': 16}
        },
        gauge={
            'shape': 'bullet',
            'axis': {
                'range': [0, 100],
                'tickmode': 'array',
                'tickvals': [0, 25, 50, 75, 100],
                'ticktext': ['0%', '25%', '50%', '75%', '100%']
            },
            'bar': {
                'color': '#2ca02c',
                'thickness': 0.4
            },
            'threshold': {
                'line': {'color': 'red', 'width': 2},
                'thickness': 0.75,
                'value': 100
            },
            'steps': [
                {'range': [0, pct],     'color': 'lightgray'},
                {'range': [pct, 100],   'color': 'gold'}
            ]
        }
    ))

    # điều chỉnh layout cho gọn
    fig_g.update_layout(
        height=120,
        margin={'t': 30, 'b': 10, 'l': 10, 'r': 10},
        template='plotly_white'
    )
    c3.plotly_chart(fig_g, use_container_width=True)

    # Line doanh thu & lợi nhuận theo tháng
    st.subheader("Doanh thu & lợi nhuận theo tháng")
    fig_line = px.line(df, x='month', y=['sales','profit'],
                       labels={'month':'Tháng','value':'Số tiền','variable':'Chỉ số'},
                       markers=True)
    st.plotly_chart(fig_line, use_container_width=True)

    # Bar Top Market
    st.subheader("Top Market Theo Doanh Thu")
    fig_bar = px.bar(df_mkt, x='sales', y='market', orientation='h',
                     labels={'sales':'Doanh thu','market':'Thị trường'},
                     text=df_mkt['sales'].apply(lambda v: f"{v/1e6:.1f}M"))
    fig_bar.update_layout(yaxis={'categoryorder':'total ascending'})
    st.plotly_chart(fig_bar, use_container_width=True)

    # --- bổ sung stacked bar Category ---
    st.subheader("Doanh thu & Lợi nhuận theo Category")
    fig_cat = px.bar(df_cat, x='category_name', y=['profit','sales'],
                     labels={'variable':'Chỉ số','value':'Số tiền'},
                     text_auto='.2s')
    fig_cat.update_layout(barmode='stack')
    st.plotly_chart(fig_cat, use_container_width=True)

    # --- bổ sung Top 10 products ---
    st.subheader("Top 10 sản phẩm bán chạy")
    fig_top = px.bar(df_top, x='product_name', y='sales_amount',
                     labels={'product_name':'Sản phẩm','sales_amount':'Doanh số'},
                     text_auto='.2s')
    fig_top.update_layout(xaxis_tickangle=-45)
    st.plotly_chart(fig_top, use_container_width=True)


@st.cache_data(show_spinner=False)
def fetch_shipping(year_from, year_to, regions, modes):
    engine = get_engine()
    conn = engine.raw_connection()

    params = [year_from, year_to]
    where = ["d.year BETWEEN ? AND ?"]

    if regions:
        ph = ",".join("?" for _ in regions)
        where.append(f"l.region IN ({ph})")
        params += regions

    if modes:
        ph = ",".join("?" for _ in modes)
        where.append(f"s.shipping_mode IN ({ph})")
        params += modes

    sql = f"""
    SELECT
      s.order_id,
      s.shipping_date,
      d.month,
      l.city,
      s.shipping_mode,
      s.days_for_shipment_scheduled,
      s.days_for_shipping_real,
      CASE
        WHEN s.days_for_shipping_real > s.days_for_shipment_scheduled THEN 1
        ELSE 0
      END AS is_late
    FROM DONHANG.dbo.Fact_Shipping AS s
    JOIN DONHANG.dbo.Dim_Date          AS d ON s.shipping_date = d.date_key
    JOIN DONHANG.dbo.Dim_Location      AS l ON s.location_id    = l.location_id
    JOIN DONHANG.dbo.Dim_Shipping_Mode AS m ON s.shipping_mode   = m.shipping_mode
    WHERE {' AND '.join(where)}
    """
    df = pd.read_sql(sql, conn, params=params)
    conn.close()
    return df


def show_shipping_performance_dashboard():
    st.header("🚚 Hiệu suất vận chuyển")
    year_from, year_to = st.sidebar.slider("Khoảng năm", 2015, 2018, (2015, 2018))
    all_regions = ["North America", "Europe", "Asia", "Africa", "Oceania"]
    regions = st.sidebar.multiselect("Chọn Region", all_regions, default=all_regions)
    modes   = st.sidebar.multiselect("Chọn Shipping Mode",
                                     ["Standard Class","Second Class","First Class","Same Day"],
                                     default=["Standard Class"])

    df = fetch_shipping(year_from, year_to, regions, modes)

    # --- Upper row: Top 10 thành phố có đơn trễ + KPI/Area ---
    col1, col2 = st.columns([2,1])

    with col1:
        st.subheader("Top 10 thành phố có đơn trễ")
        df_city = (
            df[df["is_late"] == 1]
            .groupby("city", as_index=False)
            .agg(late_count=("is_late", "sum"))
            .sort_values("late_count", ascending=False)
            .head(10)
        )
        fig_city_bar = px.bar(
            df_city,
            x="late_count", y="city",
            orientation="h",
            labels={"late_count":"Số đơn trễ","city":"Thành phố"},
            text="late_count"
        )
        fig_city_bar.update_layout(yaxis={'categoryorder':'total ascending'})
        st.plotly_chart(fig_city_bar, use_container_width=True)

    with col2:
        st.subheader("Shipments by Day & Avg Days / Late Count")
        df_day = (
            df.groupby("shipping_date", as_index=False)
              .agg(
                total_shipments=("order_id", "count"),
                late_count=("is_late", "sum"),
                avg_days=("days_for_shipping_real", "mean"),
              )
        )
        latest = df_day.iloc[-1]
        st.metric(
            "Avg Shipping Days",
            f"{latest['avg_days']:.2f}",
            delta=f"Late: {int(latest['late_count'])}, Total: {int(latest['total_shipments'])}"
        )
        fig_area = px.area(
            df_day,
            x="shipping_date", y="total_shipments",
            labels={"shipping_date":"Ngày","total_shipments":"Tổng shipments"}
        )
        st.plotly_chart(fig_area, use_container_width=True)

    # --- Lower row: Bar & Line ---
    col3, col4 = st.columns(2)

    with col3:
        st.subheader("Số đơn trễ theo Shipping Mode")
        df_mode = (
            df[df["is_late"] == 1]
            .groupby("shipping_mode", as_index=False)
            .agg(late_count=("is_late","sum"))
        )
        fig_bar = px.bar(
            df_mode,
            x="late_count", y="shipping_mode",
            orientation="h",
            labels={"late_count":"Số đơn trễ","shipping_mode":"Shipping Mode"},
            text="late_count"
        )
        st.plotly_chart(fig_bar, use_container_width=True)

    with col4:
        st.subheader("Thời gian giao trung bình theo tháng")
        df_mon = (
            df.groupby("month", as_index=False)
              .agg(avg_days=("days_for_shipping_real","mean"))
        )
        fig_line = px.line(
            df_mon,
            x="month", y="avg_days",
            labels={"month":"Tháng","avg_days":"Avg Shipping Days"}
        )
        st.plotly_chart(fig_line, use_container_width=True)


def show_product_category_dashboard():
    st.header("🛍️ Phân tích Sản phẩm & Danh mục")
    all_cats = ["Fishing","Camping & Outdoors","Computers","Electronics"]
    categories = st.sidebar.multiselect("Chọn Category", all_cats, default=all_cats[:2])
    depts = st.sidebar.multiselect("Chọn Department", ["Sports","Home","Office"], default=None)
    # TODO: Tạo fetch_product_category, rồi dùng ở đây
    st.info("Phần dữ liệu sản phẩm đang phát triển...")


def show_create_new_dashboard_form():
    st.header("⚙️ Tạo Dashboard mới")
    fact = st.selectbox("Chọn Fact Table", ["Fact_Sales","Fact_Shipping"])
    dims = st.multiselect("Chọn Dim Tables", [
        "Dim_Date","Dim_Market","Dim_Category",
        "Dim_Customer","Dim_Location","Dim_Shipping_Mode"
    ])
    chart = st.selectbox("Loại biểu đồ", ["Bar","Line","Pie","Scatter","Map"])
    x_col = st.text_input("Field trục X")
    y_col = st.text_input("Field trục Y")
    color_input = st.text_input("Field màu (tùy chọn, tên cột hoặc mã màu)")
    size_input  = st.text_input("Field kích thước (tùy chọn)")

    if st.button("Vẽ biểu đồ"):
        # Demo data
        df = pd.DataFrame({x_col:[1,2,3,4], y_col:[10,20,15,25]})
        is_color_col = color_input in df.columns
        is_size_col  = size_input  in df.columns

        if chart == 'Bar':
            if is_color_col:
                fig = px.bar(df, x=x_col, y=y_col, color=color_input)
            else:
                fig = px.bar(
                    df, x=x_col, y=y_col,
                    color_discrete_sequence=[color_input] if color_input else None
                )

        elif chart == 'Line':
            if is_color_col:
                fig = px.line(df, x=x_col, y=y_col, color=color_input)
            else:
                fig = px.line(
                    df, x=x_col, y=y_col,
                    color_discrete_sequence=[color_input] if color_input else None
                )

        elif chart == 'Pie':
            # Pie: color by column or fixed
            kwargs = {}
            if is_color_col:
                kwargs['color'] = color_input
            elif color_input:
                kwargs['color_discrete_sequence'] = [color_input]

            fig = px.pie(df, names=x_col, values=y_col, **kwargs)

        elif chart == 'Scatter':
            if is_color_col:
                fig = px.scatter(
                    df, x=x_col, y=y_col,
                    color=color_input,
                    size=size_input if is_size_col else None
                )
            else:
                fig = px.scatter(
                    df, x=x_col, y=y_col,
                    color_discrete_sequence=[color_input] if color_input else None,
                    size=size_input if is_size_col else None
                )

        else:  # Map
            if is_color_col:
                fig = px.scatter_mapbox(
                    df, lat='lat', lon='lon',
                    color=color_input,
                    size=size_input if is_size_col else None,
                    hover_name=x_col, mapbox_style='open-street-map'
                )
            else:
                fig = px.scatter_mapbox(
                    df, lat='lat', lon='lon',
                    color_discrete_sequence=[color_input] if color_input else None,
                    size=size_input if is_size_col else None,
                    hover_name=x_col, mapbox_style='open-street-map'
                )

        st.plotly_chart(fig, use_container_width=True)



# ======================
# MAIN
# ======================
def main():
    st.sidebar.title("Dashboard Selector")
    choice = st.sidebar.radio("Chọn Dashboard", [
        "Doanh thu & Lợi nhuận",
        "Hiệu suất vận chuyển",
        "Phân tích Sản phẩm & Danh mục",
        "Tạo Dashboard mới"
    ])

    if choice == "Doanh thu & Lợi nhuận":
        show_sales_profit_dashboard()
    elif choice == "Hiệu suất vận chuyển":
        show_shipping_performance_dashboard()
    elif choice == "Phân tích Sản phẩm & Danh mục":
        show_product_category_dashboard()
    else:
        show_create_new_dashboard_form()

if __name__ == '__main__':
    st.set_page_config(page_title='Multi-Dashboard App', layout='wide')
    main()
