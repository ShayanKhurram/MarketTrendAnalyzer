import psycopg2
# from jsonschema.benchmarks.const_vs_enum import value
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from sqlalchemy import create_engine
import pandas as pd
import matplotlib.pyplot as plt
import streamlit as st

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
df = pd.read_sql("product_changes", engine)
df2 = pd.read_sql("product", engine)

df['category'] = pd.merge(df, df2[['itemid', 'category']], on='itemid', how='left')['category']

# Generate the full date range
full_date_range = pd.date_range(start=df['date'].min(), end=df['date'].max())
full_date_range_df = pd.DataFrame({'date': full_date_range})

# Get unique item IDs
itemids = df['itemid'].unique()

# Prepare a list for the final data
all_rows = []

# Process each itemid
for itemid in itemids:
    # Filter data for the current itemid
    item_data = df[df['itemid'] == itemid]

    # Create a DataFrame with all dates and the current itemid
    all_dates = full_date_range_df.copy()
    all_dates['itemid'] = itemid

    # Merge to find missing dates
    merged = pd.merge(all_dates, item_data, on=['date', 'itemid'], how='left')

    # Fill missing rows with forward and backward filling
    merged = merged.ffill().bfill()

    # Add the completed rows to the list
    all_rows.append(merged)

# Combine all completed rows into a single DataFrame
final_df = pd.concat(all_rows, ignore_index=True)


# Function to calculate and predict SMA for a specific category
def calculate_sma_forecast(final_df, category, window=5, forecast_days=5, predict='itemsoldcntshow'):
    # Ensure the 'date' column is in datetime format
    final_df['date'] = pd.to_datetime(final_df['date'])

    # Filter data for the given category
    category_data = final_df[final_df['category'] == category]

    # Aggregate data by date for the category (sum for sales or price)
    category_data_aggregated = category_data.groupby('date').agg({predict: 'sum'}).reset_index()

    # Calculate SMA
    category_data_aggregated['SMA'] = category_data_aggregated[predict].rolling(window=window).mean()

    # Get the last SMA value and the second last value for trend calculation
    last_sma = category_data_aggregated['SMA'].iloc[-1]
    second_last_sma = category_data_aggregated['SMA'].iloc[-2] if len(category_data_aggregated) > 1 else last_sma

    # Calculate the trend (difference between last and second last SMA)
    trend = last_sma - second_last_sma

    # Predict the next 5 days using the trend
    forecast_dates = pd.date_range(start=category_data_aggregated['date'].iloc[-1] + pd.Timedelta(days=1),
                                   periods=forecast_days, freq='D')

    # Generate forecast values based on the trend (linear forecast)
    forecast_values = [last_sma + (i * trend) for i in range(1, forecast_days + 1)]

    # Store the forecast results in a DataFrame
    forecast_df = pd.DataFrame({'date': forecast_dates, 'forecast': forecast_values})

    return category_data_aggregated, forecast_df


# Function to print SMA predictions for a specific category
def print_forecast(final_df, category, window=5, forecast_days=5, predict='itemsoldcntshow'):
    category_sales, forecast_df = calculate_sma_forecast(final_df, category, window, forecast_days, predict)
    print(f"\n5-Day SMA Forecast for Category '{category}':")
    print(forecast_df)


# Function to plot SMA and forecast for a specific category
def plot_forecast(final_df, category, window=5, forecast_days=5, predict='itemsoldcntshow'):
    category_sales, forecast_df = calculate_sma_forecast(final_df, category, window, forecast_days, predict)

    # Create the plot with a figure and axis
    fig, ax = plt.subplots(figsize=(10, 6))

    # Plot the actual sales/price, SMA, and forecast on the same axes
    ax.plot(category_sales['date'], category_sales[predict], label=f"Actual {predict}", color='black')
    ax.plot(category_sales['date'], category_sales['SMA'], label=f"{window}-Day SMA", color='black',linestyle='--')


    # Add title, labels, and legend



    for spine in ax.spines.values():
        spine.set_visible(False)

    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'{x:.2f}'))
    # Pass the figure to Streamlit's pyplot function
    st.pyplot(fig)





# Function to calculate SMA for a specific itemid
def calculate_sma_for_item(df, itemid, window=5, forecast_days=5):
    # Filter data for the given itemid
    item_data = df[df['itemid'] == itemid]

    # Aggregate sales by date for the item
    item_sales = item_data.groupby('date').agg({'itemsoldcntshow': 'sum'}).reset_index()

    # Calculate SMA for the specified window
    item_sales['SMA'] = item_sales['itemsoldcntshow'].rolling(window=window).mean()

    # Get the last 5 distinct SMA values
    distinct_sma_values = item_sales['SMA'].dropna().unique()[-5:]

    # Calculate the trend (difference between last SMA and second last SMA)
    last_sma = distinct_sma_values[-1] if len(distinct_sma_values) >= 1 else 0
    second_last_sma = distinct_sma_values[-2] if len(distinct_sma_values) > 1 else last_sma
    trend = last_sma - second_last_sma

    # Predict the next 5 days using the trend
    forecast_dates = pd.date_range(start=item_sales['date'].iloc[-1] + pd.Timedelta(days=1),
                                   periods=forecast_days, freq='D')

    # Generate forecast values based on trend
    forecast_values = [last_sma + (i * trend) for i in range(1, forecast_days + 1)]

    # Calculate total forecasted value
    total_forecasted_value = sum(forecast_values)

    return total_forecasted_value, distinct_sma_values


# Function to process the DataFrame, sum values, and calculate forecast for top 5 itemids
def get_top_5_forecast(df, category, column='itemsoldcntshow', window=5, forecast_days=5):
    # Ensure the 'date' column is in datetime format
    df['date'] = pd.to_datetime(df['date'])

    # Filter data by category
    category_df = df[df['category'] == category]

    # Group by itemid and sum the specified column, sort in descending order
    total_sales_per_item = category_df.groupby('itemid')[column].sum().reset_index()
    total_sales_per_item_sorted = total_sales_per_item.sort_values(by=column, ascending=False)

    # Get top 5 itemids (initial selection)
    top_5_itemids = total_sales_per_item_sorted.head(5)['itemid']

    forecast_results = []

    # Calculate SMA and forecast for each of the top 5 itemids
    for itemid in top_5_itemids:
        total_forecasted_value, distinct_sma_values = calculate_sma_for_item(category_df, itemid, window, forecast_days)

        # Skip items with zero forecasted values
        if total_forecasted_value == 0:
            continue

        # Calculate the mean price for the item within the category
        mean_price = category_df[category_df['itemid'] == itemid]['price'].mean()

        # Append results
        forecast_results.append({
            'itemid': itemid,
            'category': category,
            'forecasted_value': total_forecasted_value,
            'price': mean_price
        })

    # If fewer than 5 items with non-zero forecasted values, select the next best itemids
    remaining_itemids = total_sales_per_item_sorted.loc[
        ~total_sales_per_item_sorted['itemid'].isin([item['itemid'] for item in forecast_results])]

    # Add additional items with non-zero forecasted values until we have 5
    for itemid in remaining_itemids['itemid']:
        if len(forecast_results) >= 5:
            break

        total_forecasted_value, distinct_sma_values = calculate_sma_for_item(category_df, itemid, window, forecast_days)

        # Skip if forecasted value is still zero
        if total_forecasted_value == 0:
            continue

        # Calculate the mean price for the item
        mean_price = category_df[category_df['itemid'] == itemid]['price'].mean()

        # Append results
        forecast_results.append({
            'itemid': itemid,
            'category': category,
            'forecasted_value': total_forecasted_value,
            'price': mean_price
        })

    # Ensure exactly 5 items are returned
    if len(forecast_results) < 5:
        forecast_results = forecast_results[:5]  # Return only the first 5 items

    # Convert results to DataFrame
    forecast_df = pd.DataFrame(forecast_results)

    return forecast_df







