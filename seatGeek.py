import os
import json
import requests
import pandas as pd
from postgrest import APIError
from dotenv import load_dotenv
from bs4 import BeautifulSoup as Soup
from supabase import create_client, Client
load_dotenv()

def generate_urls(url, start_page, end_page):
    urls = []
    for page in range(start_page, end_page + 1):
        new_url = url.replace("page=1", "page=" + str(page))
        urls.append(new_url)
    return urls


def fetch_seatgeek_data(url, start_page, end_page):
    generated_urls = generate_urls(url, start_page, end_page)
    df_list = []

    for url in generated_urls:
        # Load the data from the URL
        response = requests.get(url)

        # Convert the response content to a JSON object
        data = json.loads(response.content)

        # Extract the events list from the JSON object
        events = data["events"]

        # Create a Pandas DataFrame from the events list
        df = pd.json_normalize(events)

        cols = ['type',
                'id',
                'datetime_utc',
                'performers',
                'is_open',
                'datetime_local',
                'time_tbd',
                'short_title',
                'visible_until_utc',
                'url',
                'score',
                'mobile_entry_enabled',
                'announce_date',
                'created_at',
                'date_tbd',
                'title',
                'popularity',
                'status',
                'conditional',
                'last_valid_datetime_utc',
                'last_valid_datetime_local',
                'schedule_status',
                'visible_at',
                'is_visible_override',
                'relative_url',
                'is_hybrid',
                'show_static_map_images',
                'all_in_price_before_checkout',
                'venue.state',
                'venue.name_v2',
                'venue.postal_code',
                'venue.name',
                'venue.timezone',
                'venue.url',
                'venue.score',
                'venue.location.lat',
                'venue.location.lon',
                'venue.address',
                'venue.country',
                'venue.has_upcoming_events',
                'venue.num_upcoming_events',
                'venue.city',
                'venue.slug',
                'venue.extended_address',
                'venue.id', 'venue.popularity',
                'venue.metro_code',
                'venue.capacity',
                'venue.display_location',
                'venue_config.map_config_id',
                'venue_config.is_mapped',
                'venue_config.seatingchart_url',
                'venue_config.event_type',
                'venue_config.is_ga',
                'venue_config.document_source.source_type',
                'venue_config.document_source.generation_type',
                'venue_config.seat_selection_enabled',
                'venue_config.has_seatview',
                'stats.listing_count',
                'stats.average_price',
                'stats.lowest_price_good_deals',
                'stats.lowest_price',
                'stats.highest_price',
                'stats.visible_listing_count',
                'stats.median_price',
                'stats.lowest_sg_base_price',
                'stats.lowest_sg_base_price_good_deals']
        df = df[cols]
        df_list.append(df)

    data = pd.concat(df_list, axis=0)

    # Extract 'type', 'name', and 'image' from 'performers' column
    data[['performer_type', 'performer_name', 'image_url']] = data['performers'].apply(
        lambda x: pd.Series([x[0]['type'], x[0]['name'], x[0]['image']]))

    # Drop the original 'performers' column
    data.drop('performers', axis=1, inplace=True)
    data.columns = [i.replace('_config.', '.') for i in data.columns]
    # Resetting the index
    data.reset_index(drop=True, inplace=True)
    data.fillna('Nan', inplace = True)
    data.drop_duplicates(inplace = True)
    # Return data
    return data

url = "https://seatgeek.com/api/events?page=1&sort=datetime_local.asc&taxonomies.id=2000000&client_id=MTY2MnwxMzgzMzIwMTU4"
supabase_url: str = os.getenv("SUPABASE_URL")
supabase_key: str = os.getenv("SUPABASE_KEY")

supabase_client: Client = create_client(supabase_url, supabase_key)

def insert_records_to_supabase(json_data):
    for record in json_data:
        try:
            supabase_client.table('seatgeek_data').insert(record).execute()
        except APIError as e:
            print("Error occurred while inserting the record:")
            print(record)
            print("Error message:", e.message)
            print()


json_data = fetch_seatgeek_data(url, 1, 5).to_dict(orient='records')
insert_records_to_supabase(json_data)


def remove_duplicate_rows(table_name):
    response = supabase_client.table(table_name).select('*').execute()
    data = response.data

    # Create a set to store unique IDs
    unique_ids = set()

    # List to store rows to be deleted
    rows_to_delete = []

    # Iterate through each row and filter duplicates based on 'id'
    for row in data:
        row_id = row['id']
        if row_id not in unique_ids:
            unique_ids.add(row_id)
        else:
            rows_to_delete.append(row_id)

    # Delete the duplicate rows from the table and retain the first occurrence
    for row_id in rows_to_delete:
        # Retrieve the first occurrence of the duplicate row
        first_occurrence = next(row for row in data if row['id'] == row_id)
        # Delete all other occurrences of the duplicate row
        supabase_client.table(table_name).delete().match({'id': row_id}).execute()
        # Insert the first occurrence back into the table
        supabase_client.table(table_name).insert(first_occurrence).execute()

table_name = 'seatgeek_data'
remove_duplicate_rows(table_name)
