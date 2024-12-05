import pandas as pd
import json
import requests
import time
from datetime import datetime
from sqlalchemy import create_engine
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from container import tracking

# this function will be used for filtering required data from json format data
def extract_all_details(data_dict):
    # Lists to store each feature
    prices = []
    item_sold_count = []
    names = []
    in_stock = []
    discounts = []
    rating_scores = []
    reviews = []
    itemids = []

    # Iterate through all list items in data_dict["mods"]["listItems"]
    for item in data_dict["mods"]["listItems"]:
        # Extract and append price

        if "itemId" in item:
            itemids.append(item["itemId"])
        else:
            itemids.append(None)

        if "price" in item:
            prices.append(item["price"])
        else:
            prices.append(None)  # Append None if price is not available

        # Extract and append item sold count
        if "itemSoldCntShow" in item:
            item_sold_count.append(item["itemSoldCntShow"])
        else:
            item_sold_count.append(None)

        # Extract and append name
        if "name" in item:
            names.append(item["name"])
        else:
            names.append(None)

        # Extract and append in stock status
        if "inStock" in item:
            in_stock.append(item["inStock"])
        else:
            in_stock.append(None)

        # Extract and append discount
        if "discount" in item:
            discounts.append(item["discount"])
        else:
            discounts.append(None)

        # Extract and append rating score
        if "ratingScore" in item:
            rating_scores.append(item["ratingScore"])
        else:
            rating_scores.append(None)

        # Extract and append review count
        if "review" in item:
            reviews.append(item["review"])
        else:
            reviews.append(None)

    # Return a dictionary with all the extracted details
    return {
        "price": prices,
        "itemSoldCntShow": item_sold_count,
        "name": names,
        "inStock": in_stock,
        "discount": discounts,
        "ratingScore": rating_scores,
        "review": reviews,
        "itemId": itemids
    }

# this function will scrape first 50 pages of  e-commerce website

def scrape_first_50_pages(item):
    all_details = {
        "price": [],
        "itemSoldCntShow": [],
        "name": [],
        "inStock": [],
        "discount": [],
        "ratingScore": [],
        "review": [],
        "itemId": []
    }

    for page in range(1, 51):  # Loop through the first 50 pages
        url = f'https://www.daraz.pk/catalog/?ajax=true&isFirstRequest=true&page={page}&q={item}'

        try:
            response = requests.get(url)

            # Check if the request was successful
            if response.status_code == 200:
                # Check if response is HTML (indicating an error page or rate limit)
                if response.headers['Content-Type'].startswith('text/html'):
                    print(f"Received HTML response on page {page}, possibly rate-limited. Retrying with longer delay.")
                    time.sleep(10)  # Wait longer before retrying
                    continue  # Skip to the next iteration (page)

                try:
                    data_updated = response.json()  # Parse JSON

                    # Extract details from the current page
                    page_details = extract_all_details(data_updated)

                    # Append each detail to the respective list in all_details
                    for key in all_details:
                        all_details[key].extend(page_details.get(key, []))

                except json.JSONDecodeError:
                    print(
                        f"Failed to decode JSON on page {page}. Response text: {response.text[:100]}")  # Log first 100 characters

            else:
                print(f"Failed to retrieve page {page}, status code: {response.status_code}")

        except requests.RequestException as e:
            print(f"Error occurred while requesting page {page}: {str(e)}")

        # Optional: Delay to avoid rate-limiting
        time.sleep(10)  # Increase delay to 3 seconds between requests

    return all_details

# list of objects we are currently tracking

updated = [scrape_first_50_pages(x) for x in tracking ]



# more processing of data adding category and date column


def process_scraped_data(json_data, product_names):
    # Create an empty list to store individual product dataframes
    dataframes = []

    # Iterate through the JSON data and the product names
    for data, product_name in zip(json_data, product_names):
        # Create a DataFrame for each product's data
        # Assuming the JSON data contains the necessary fields as lists
        df = pd.DataFrame({
            'Price': data['price'],
            'ItemSoldCntShow': data['itemSoldCntShow'],
            'Name': data['name'],
            'InStock': data['inStock'],
            'Discount': data['discount'],
            'RatingScore': data['ratingScore'],
            'Review': data['review'],
            'Itemid': data['itemId']
        }, columns=[
            'Itemid', 'Price', 'ItemSoldCntShow', 'Name', 'InStock',
            'Discount', 'RatingScore', 'Review'
        ])

        # Add a 'Category' column with the product name
        df['Category'] = product_name

        # Append the dataframe to the list
        dataframes.append(df)

    # Combine all DataFrames into a single DataFrame
    combined_df = pd.concat(dataframes, ignore_index=True)

    # Add today's date as a 'Date' column
    today_date = datetime.today().strftime('%Y-%m-%d')
    combined_df['Date'] = today_date

    return combined_df


# Process the scraped data
result_df = process_scraped_data(updated, tracking)
print(result_df)

result_df = result_df.reindex(columns=[
    'Date','Category','Itemid', 'Price', 'ItemSoldCntShow', 'Name', 'InStock',
    'Discount', 'RatingScore', 'Review'
])

# DATA pre-processing
result_df["ItemSoldCntShow"] = result_df["ItemSoldCntShow"].fillna(0)
result_df['Review'] = result_df['Review'].replace('', '0')
result_df['Discount'] = result_df['Discount'].str.replace('% Off', '').str.strip()
result_df['Discount'] = result_df['Discount'].fillna(0)
result_df['ItemSoldCntShow'] = result_df['ItemSoldCntShow'].str.replace('.', '').str.strip()
result_df['ItemSoldCntShow'] = result_df['ItemSoldCntShow'].str.replace('K sold', '00').str.strip()
result_df['ItemSoldCntShow'] = result_df['ItemSoldCntShow'].str.replace(' sold', '').str.strip()
result_df['Price'] = result_df['Price'].astype(float)
result_df['ItemSoldCntShow'] = result_df['ItemSoldCntShow'].astype(float)
result_df['Discount'] = result_df['Discount'].astype(int)
result_df['ItemSoldCntShow'] = result_df['ItemSoldCntShow'].fillna(0)
result_df['ItemSoldCntShow'] = result_df['ItemSoldCntShow'].astype(int)
# Step 1: Use apply() to replace empty strings with '0'
result_df['RatingScore'] = result_df['RatingScore'].apply(lambda x: '0' if x == '' else x)
result_df['ItemSoldCntShow'] = result_df['ItemSoldCntShow'].astype(int)
result_df['RatingScore'] = result_df['RatingScore'].astype(float)
result_df['Review'] = result_df['Review'].astype(int)
result_df['Itemid'] = result_df['Itemid'].astype(int)
result_df['Date']= pd.to_datetime(result_df['Date'])
result_df.columns = result_df.columns.str.lower()


# working with database
engine = create_engine('postgresql://postgres:1100@localhost:5432/products', isolation_level='AUTOCOMMIT')
pgcon = psycopg2.connect(
    host="localhost",
    user="postgres",
    password="1100",
    database = 'products'
)
pgcursor = pgcon.cursor()
pgcon.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
engine.raw_connection()

# importing db table as df
db_data = pd.read_sql("product",engine)


def update_and_track_changes(today, first_day, changes=None):
    """
    Compares today's dataframe with the first day's dataframe,
    creates an updated copy of the first day's dataframe with any changes in price or items sold,
    and stores those changes in a changes dataframe, including the previous price and previous sold count.

    Parameters:
        today (pd.DataFrame): DataFrame with today's data.
        first_day (pd.DataFrame): DataFrame with the first day's data.
        changes (pd.DataFrame or None): DataFrame to store changes. If None, a new DataFrame is created.

    Returns:
        pd.DataFrame: The updated changes dataframe.
        pd.DataFrame: A new updated copy of the first_day dataframe.
    """
    # Create a copy of first_day to update without modifying the original
    updated_first_day = first_day.copy()

    # Initialize changes if it's None
    if changes is None:
        changes = pd.DataFrame(
            columns=['itemid', 'date', 'itemsoldcntshow', 'price', 'previous_price', 'previous_sold'])

    # Iterate over today's data and compare with first day data
    for idx, row in today.iterrows():
        itemid = row['itemid']
        today_itemsoldcntshow = row['itemsoldcntshow']
        today_price = row['price']

        # Find the corresponding row in the first_day dataframe
        first_day_row = updated_first_day[updated_first_day['itemid'] == itemid]
        if not first_day_row.empty:
            first_itemsoldcntshow = first_day_row.iloc[0]['itemsoldcntshow']
            first_price = first_day_row.iloc[0]['price']

            # Check for changes in price or items sold count
            if (today_itemsoldcntshow != first_itemsoldcntshow) or (today_price != first_price):
                # Update the updated_first_day dataframe with new values
                updated_first_day.loc[updated_first_day['itemid'] == itemid, 'itemsoldcntshow'] = today_itemsoldcntshow
                updated_first_day.loc[updated_first_day['itemid'] == itemid, 'price'] = today_price

                # Store the changes in the changes dataframe, including previous price and previous sold count
                change_data = {
                    'itemid': itemid,
                    'date': row['date'],
                    'itemsoldcntshow': today_itemsoldcntshow,
                    'price': today_price,
                    'previous_price': first_price,  # Store the previous price
                    'previous_sold': first_itemsoldcntshow  # Store the previous sold count
                }
                changes = pd.concat([changes, pd.DataFrame([change_data])], ignore_index=True)

    return changes, updated_first_day

changes, updated_first_day = update_and_track_changes(result_df,db_data, changes=None)

changes["previous_sold"] = changes["previous_sold"].astype(int)
changes["itemsoldcntshow"] = changes["itemsoldcntshow"].astype(int)
changes["itemid"] = changes["itemid"].astype(int)

changes.to_sql('product_changes', engine, if_exists='append', index=False)
pgcursor.execute("TRUNCATE TABLE PRODUCT")
pgcon.commit()
updated_first_day.to_sql('product', engine, if_exists='append', index=False)
print("done")