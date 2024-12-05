from operator import index
import psycopg2
# from jsonschema.benchmarks.const_vs_enum import value
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import streamlit as st
from streamlit_option_menu import option_menu
from sqlalchemy import create_engine
import pandas as pd
import plotly.express as px
import plotly
import streamlit_shadcn_ui as ui
from container import tracking
import matplotlib.pyplot as plt
import time
from datetime import datetime
import model as md


st.set_page_config(layout="wide")
col1, col2, col3 = st.columns([1, 2, 1])

st.markdown("<h1 style='text-align: center;'>Market Trend Analyzer</h1>", unsafe_allow_html=True)

st.write("")
st.write("")
st.write("")
st.write("")
selected = option_menu(
        menu_title=None,
        options=["Add Product","Dashboard","Category Tracker","Future Trends"],
        icons=["plus-circle", "bar-chart", "clipboard-data", "graph-up"],
        menu_icon="cast",
        default_index=1,
        orientation = "horizontal",
        styles={
        "container": {
            "padding": "0!important",
            "background-color": "#fafafa",
            "display": "flex"
        },
        "nav-link": {
            "font-size": "18px",
            "text-align": "left",
            "margin": "0px",
            "padding": "10px 20px",  # Adjusted padding for balanced spacing
            "color": "black",  # Ensures text is visible on light background
            "--hover-color": "#ddd"  # Slightly darker hover color
        },
        "icon": {
            "color": "black",  # Icon color
            "font-size": "20px",  # Icon size
        },
        "nav-link-selected": {
            "background-color": "black",
            "color": "white",
            "font-weight": "bold"  # Makes selected text stand out
        },

    }

    )
# connecting to database
engine = create_engine('postgresql://postgres:1100@localhost:5432/products', isolation_level='AUTOCOMMIT')
pgcon = psycopg2.connect(
    host="localhost",
    user="postgres",
    password="1100",
    database='products'
)
pgcursor = pgcon.cursor()
pgcon.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
engine.raw_connection()

# importing db table as df
db_data = pd.read_sql("product_changes", engine)
db = pd.read_sql("product", engine)

# Merge df2 with df1 on 'item id' to get 'category' in df2
result = db_data.merge(db[['itemid', 'category','name']], on='itemid', how='left')
result= result.drop_duplicates(subset=['date', 'itemid']).reset_index(drop=True)
result["diff_sales"] = result["itemsoldcntshow"] - result["previous_sold"]
condition = result["diff_sales"]<0
result_modified = result[~condition]
ecom_sales = result_modified.groupby(['date', 'category'])['itemsoldcntshow'].agg('sum').reset_index(name='itemsoldcntshow')

# Create the line graph
line_graph = px.line(
    # Set the appropriate DataFrame and title
    data_frame=ecom_sales,
    # Set the x and y arguments
    x='date', y='itemsoldcntshow',
    # Ensure a separate line per country
    color='category', width=1100, height=400)

ecom_prices = result_modified.groupby(['date', 'category'])['price'].agg('mean').reset_index(name='price')

custom_colors = ['#1a1a1a', '#4b0082', '#8b0000', '#006400', '#2f4f4f']

line_graphs = px.line(
    # Set the appropriate DataFrame and title
    data_frame=ecom_prices,
    # Set the x and y arguments
    x='date', y='price',color_discrete_sequence=custom_colors,
    # Ensure a separate line per country
    color='category', width=1100, height=400)

line_graphs.update_layout(
    xaxis_title_font=dict(color='black'),    # X-axis title color
    yaxis_title_font=dict(color='black'),    # Y-axis title color
    xaxis_tickfont=dict(color='black'),      # X-axis tick label color
    yaxis_tickfont=dict(color='black')       # Y-axis tick label color
)
ecom_top_sales = result_modified.groupby(['date'])['diff_sales'].agg('sum').reset_index(name='diff_sales')
line_graphs_top_sales = px.line(
    # Set the appropriate DataFrame and title
    data_frame=ecom_top_sales,
    # Set the x and y arguments
    x='date', y='diff_sales', width=1100, height=400, color_discrete_sequence=['black'])
# Customize the x and y axis tick colors to black
line_graphs_top_sales.update_xaxes(title_font=dict(color='black'), tickfont=dict(color='black'))
line_graphs_top_sales.update_yaxes(title_font=dict(color='black'), tickfont=dict(color='black'))


result_modified_sorted = result_modified.sort_values(by='date', ascending=True)
result_modified_sorted.drop(['itemsoldcntshow','previous_price','previous_sold','date'],axis=1)
result_modified_sorted = result_modified_sorted[['itemid','name','category','diff_sales','price']]
result_modified_sorted = result_modified_sorted.rename(columns={'diff_sales':'sales'})
result_modified_sorted = result_modified_sorted.groupby('itemid', as_index=False).agg({'sales': 'sum','category': 'last',      # Take the first category for each itemid
    'price': 'last',         # Take the first price for each itemid
    'name': 'last'})
result_modified_sorted = result_modified_sorted.sort_values(by='sales', ascending=False)


total_sales_by_category = result_modified.groupby('category')['diff_sales'].sum().reset_index()
result_modified['revenue'] =  result_modified['price']*result_modified['diff_sales']
total_revenue_by_category = result_modified.groupby('category')['revenue'].sum().reset_index()
average_price_by_category = result_modified.groupby('category')['price'].mean().reset_index()


def histoplot(user_category):
    # Filter DataFrame by category
    filtered_df = result_modified[result_modified['category'] == user_category]

    # Group by date and sum sales for each date
    sales_by_date = filtered_df.groupby('date')['diff_sales'].sum()

    # Plotting the sales per date as a bar plot
    fig, ax = plt.subplots(figsize=(10, 4))
    ax.bar(sales_by_date.index, sales_by_date.values, color='black', edgecolor='black')
    ax.set_xlabel('Date')
    ax.set_ylabel('Number of Sales')
    plt.xticks(rotation=0)
    plt.tight_layout()
    # Set x and y axis tick parameters to black with smaller font size
    ax.tick_params(axis='x', colors='black', labelsize=8)  # Smaller font for x-axis
    ax.tick_params(axis='y', colors='black', labelsize=8)  # Smaller font for y-axis
    # Remove borders (spines)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_visible(False)
    ax.spines['bottom'].set_visible(False)

    return fig


def format_revenue(value_series):
    """
    Checks if a Pandas Series is not empty, extracts the first element,
    and formats it as currency. Returns "Not Available" if the Series is empty.
    """
    if not value_series.empty:
        revenue_value = value_series.iloc[0]  # Extract the single numeric value
        return f"{revenue_value:,.2f}"  # Format as currency with commas and two decimal places
    else:
        return "Not Available"

today = datetime.today().date()

# Filter dates less than or equal to today's date, drop duplicates, and sort in descending order
unique_dates = result[result['date'] <= pd.Timestamp(today)]['date'].drop_duplicates().sort_values(ascending=False)

# Get the last two dates
last_two_dates = unique_dates.head(2).tolist()



sales_by_date_category = result_modified.groupby(['date', 'category'])[['diff_sales','revenue']].sum().reset_index()
avg_by_date_category = result_modified.groupby(['date', 'category'])['price'].mean().reset_index()

filtered_sales_by_date_category_FF = sales_by_date_category[~sales_by_date_category["date"].isin(last_two_dates)]
filtered_sales_by_date_category_TT = sales_by_date_category[sales_by_date_category["date"].isin(last_two_dates)]
filtered_sales_by_date_category_F=filtered_sales_by_date_category_FF.groupby('category')['diff_sales'].sum().reset_index()
filtered_sales_by_date_category_T=filtered_sales_by_date_category_TT.groupby('category')['diff_sales'].sum().reset_index()

total_revenue_by_categorysF = filtered_sales_by_date_category_FF.groupby('category')['revenue'].sum().reset_index()
total_revenue_by_categorysT = filtered_sales_by_date_category_TT.groupby('category')['revenue'].sum().reset_index()

filtered_avg_price_FF = avg_by_date_category[~avg_by_date_category["date"].isin(last_two_dates)]
filtered_avg_price_TT = avg_by_date_category[avg_by_date_category["date"].isin(last_two_dates)]


avg_price__by_categorysF = filtered_avg_price_FF.groupby('category')['price'].mean().reset_index()
avg_price_by_categorysT = filtered_avg_price_FF.groupby('category')['price'].mean().reset_index()

# Creating the bar chart and storing it in a variable
fig, ax = plt.subplots(figsize=(10, 6))
ax.barh(total_sales_by_category['category'], total_sales_by_category['diff_sales'], color='black')
ax.set_xlabel('Sales')
ax.set_ylabel('Category')

ax.invert_yaxis()  # Optional: invert y-axis to have the highest value at the top
for spine in ax.spines.values():
    spine.set_visible(False)



def  calculate_avg(all_revenue,new_revenue):
    c= (((all_revenue - new_revenue)/all_revenue)*100)
    return c
def value_finder(choices,cal):

    if(cal=="sales"):
        value11 = filtered_sales_by_date_category_F.loc[filtered_sales_by_date_category_F['category'].str.lower() == choices, 'diff_sales']
        value12 = filtered_sales_by_date_category_T.loc[filtered_sales_by_date_category_T['category'].str.lower() == choices, 'diff_sales']


    if(cal=="revenue"):
        value11 = total_revenue_by_categorysF.loc[total_revenue_by_categorysF['category'].str.lower() == choices, 'revenue']
        value12 = total_revenue_by_categorysT.loc[total_revenue_by_categorysT['category'].str.lower() == choices, 'revenue']

    if(cal=="avg"):
        value11 = avg_price__by_categorysF.loc[avg_price__by_categorysF['category'].str.lower() == choices, 'price']
        value12 = avg_price_by_categorysT.loc[avg_price_by_categorysT['category'].str.lower() == choices, 'price']


    return value11,value12

def update_tracking_file(tracking_list):
    with open("container.py", "w") as f:
        f.write("tracking = " + str(tracking_list))



if selected=="Dashboard" :
    col1,col2 = st.columns(2)
    col3,col4 = st.columns(2)

    # working with database
    with col1:
        st.write("")
        st.write("")
        st.write("")




        st.markdown("<h3 style='text-align: left; font-size:18px;'>Sales by category</h3>", unsafe_allow_html=True)
        st.write("")
        st.write("")
        st.write("")
        st.pyplot(fig)

    with col2:
        st.write("")
        st.write("")
        st.write("")

        st.markdown("<h3 style='text-align: left; font-size:18px;'>Sales by Days</h3>", unsafe_allow_html=True)
        st.plotly_chart(line_graphs_top_sales)

    with col3:
        st.write("")
        st.write("")
        st.write("")
        st.markdown("<h3 style='text-align: left; font-size:18px;'>Price by Category and Days</h3>", unsafe_allow_html=True)
        st.plotly_chart(line_graphs)

    with col4:
        st.write("")
        st.write("")
        st.write("")


        st.markdown("<h3 style='text-align: left; font-size:18px;'>Top Products</h3>", unsafe_allow_html=True)
        st.dataframe(result_modified_sorted)


if selected=="Category Tracker":
    choice = ui.select(options=tracking)
    value1 = total_revenue_by_category.loc[total_revenue_by_category['category'].str.lower() == choice, 'revenue']
    formatted_revenue =   format_revenue(value1)
    value2 = total_sales_by_category.loc[total_sales_by_category['category'].str.lower() == choice, 'diff_sales']
    formatted_sales = format_revenue(value2)
    value3 = average_price_by_category.loc[average_price_by_category['category'].str.lower() == choice, 'price']
    formatted_avg = format_revenue(value3)
    alll_revenue,days_revenue = value_finder(choice,"sales")
    sale_change = calculate_avg(alll_revenue,days_revenue)
    formatted_sale_change= format_revenue(sale_change)
    alll_sales, days_sales = value_finder(choice, "revenue")
    revenue_change = calculate_avg(alll_sales,days_sales)
    formatted_revenue_change = format_revenue(revenue_change)
    all_avg,days_avg = value_finder(choice,"avg")
    avg_change = all_avg-days_avg
    formatted_avg_change=format_revenue(avg_change)
    # formatted_average = format_revenue(value3)
    cols = st.columns(3)
    col11 = st.columns(1)[0]
    col12= st.columns(1)[0]
    with cols[0]:

        ui.metric_card(title="Total Revenue", content=f"Rs {formatted_revenue}", description=f"+{formatted_sale_change}% from last two days", key="card1")
    with cols[1]:
        ui.metric_card(title="Total Sales", content=f"{formatted_sales}", description=f"+{formatted_revenue_change}% from last two days", key="card2")
    with cols[2]:
        ui.metric_card(title="Average Price", content=f"Rs {formatted_avg}", description=f"+{formatted_avg_change}% from two days", key="card3")

    with col11:
        st.write("")
        st.write("")
        st.write("")
        st.markdown("<h3 style='text-align: left; font-size:24px;'>Sales by Days</h3>", unsafe_allow_html=True)
        st.write("")
        hist_plot=histoplot(choice)
        st.pyplot(hist_plot)
    with col12:
        df = result_modified[result_modified["category"] == choice]
        df_sorted = df.sort_values(by='diff_sales', ascending=False)
        df_sorted = df_sorted[['itemid', 'name', 'diff_sales', 'price']]
        df_sorted = df_sorted.rename(columns={'diff_sales': 'sales'})
        st.markdown("<h3 style='text-align: left; font-size:24px;'>Top Products</h3>", unsafe_allow_html=True)
        st.dataframe(df_sorted)

if selected=="Add Product":
    st.markdown(
        """
        <h1 style='text-align: left;font-size:30px; font-weight: bold;'>Tracking Products</h1>
        """,
        unsafe_allow_html=True
    )
    st.markdown("\n".join([f"- {item}" for item in tracking]))
    st.title("Add a product")
    st.write("")
    st.write("")

    # Add product
    query1 = st.text_input("Enter a product you want to track:")
    if query1:
        if query1 not in tracking:
            tracking.append(query1)
            update_tracking_file(tracking)  # Update the file
            st.write(f"'{query1}' has been added to the tracking list.")
        else:
            st.write(f"'{query1}' is already being tracked.")

    # Remove product
    st.write("")
    st.title("Remove a product")
    st.write("")

    query2 = st.text_input("Enter a product you want to untrack:")
    if query2:
        if query2 in tracking:
            tracking.remove(query2)
            update_tracking_file(tracking)  # Update the file
            st.write(f"'{query2}' has been removed from the tracking list.")
        else:
            st.write(f"'{query2}' is not in the tracking list.")

if  selected=="Future Trends":
    choice = ui.select(options=tracking)
    cols = st.columns(3)
    col1,col2 = st.columns(2)
    col3,col4  =st.columns(2)
    with cols[0]:

        ui.metric_card(title="Best Forecasted Demand", content="fde", description="efef", key="card1")
    with cols[1]:
        ui.metric_card(title="Total Sales", content="efe", description=" from last two days", key="card2")
    with cols[2]:
        ui.metric_card(title="Average Price", content="Rs", description="m two days", key="card3")


    st.write("")
    st.write("")
    st.write("")
    st.write("")
    st.write("")
    st.write("")
    st.write("")
    st.write("")

    with col1:
        st.write("")
        st.write("")
        st.write("")
        st.write("")
        st.write("")
        st.write("")
        st.write("")
        st.write("")
        st.markdown("<h3 style='text-align: left; font-size:18px;'>Price Predicted for next five days</h3>",unsafe_allow_html=True)
        md.plot_forecast(md.final_df,choice, window=5, forecast_days=5, predict="price" )
    with col2:
        st.write("")
        st.write("")
        st.write("")
        st.write("")
        st.write("")
        st.write("")
        st.write("")
        st.write("")
        st.markdown("<h3 style='text-align: left; font-size:18px;'>Sales Predicted for next five days</h3>",unsafe_allow_html=True)
        md.plot_forecast(md.final_df, choice, window=5, forecast_days=5, predict="itemsoldcntshow")
    with col3:
        # Example usage
        # Assuming df is your DataFrame with 'itemid', 'date', 'itemsoldcntshow', 'category', and 'price' columns
        forecast_df = md.get_top_5_forecast(md.final_df, category=choice, column='price', window=5,
                                         forecast_days=5)
        st.dataframe(forecast_df)




