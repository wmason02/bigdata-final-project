# Will Mason
# data from https://catalog.data.gov/dataset/motor-vehicle-collisions-crashes/resource/b5a431d2-4832-43a6-9334-86b62bdb033f

import pandas as pd
import logging
from pymongo import MongoClient

# Setup logging
logging.basicConfig(filename='data_import.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def process_chunk(chunk):
    # Data cleaning and processing
    chunk['CRASH DATE'] = pd.to_datetime(chunk['CRASH DATE'], errors='coerce')
    chunk.fillna({'BOROUGH': 'Unknown', 'ZIP CODE': 'Unknown'}, inplace=True)
    chunk.dropna(subset=['LATITUDE', 'LONGITUDE'], inplace=True)
    return chunk.to_dict('records')

# MongoDB connection setup
client = MongoClient('mongodb://localhost:27017/')
db = client['traffic_db']
collection = db['collisions']

file_path = 'data.csv'
chunk_size = 50000

try:
    # Read CSV in chunks and insert data into MongoDB
    for chunk in pd.read_csv(file_path, chunksize=chunk_size, dtype={'ZIP CODE': str}, low_memory=False):
        records = process_chunk(chunk)
        collection.insert_many(records) 
        logging.info(f"Inserted {len(records)} records into MongoDB")
except Exception as e:
    logging.error(f"An error occurred: {str(e)}")
finally:
    client.close()
    logging.info("MongoDB connection closed and script completed")
