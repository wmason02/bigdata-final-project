# Will Mason

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pymongo import MongoClient
import logging

# Setup logging
logging.basicConfig(filename='data_visualization.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# MongoDB connection setup
client = MongoClient('mongodb://localhost:27017/')
db = client['traffic_db']
collection = db['collisions']

try:
    # Fetch data from MongoDB
    data = pd.DataFrame(list(collection.find()))
    logging.info("Data successfully loaded from MongoDB")

    # Clean and preprocess data
    data = data.dropna(subset=['LATITUDE', 'LONGITUDE'])
    data = data[(data['LATITUDE'] != 0) & (data['LONGITUDE'] != 0)]

    # Zoom into a range of longitudes and latitudes
    longitude_range = [data['LONGITUDE'].quantile(0.05), data['LONGITUDE'].quantile(0.95)]
    latitude_range = [data['LATITUDE'].quantile(0.05), data['LATITUDE'].quantile(0.95)]

    data = data[(data['LONGITUDE'] >= longitude_range[0]) & (data['LONGITUDE'] <= longitude_range[1])]
    data = data[(data['LATITUDE'] >= latitude_range[0]) & (data['LATITUDE'] <= latitude_range[1])]

    # Visualization 1: Number of Collisions by Borough
    plt.figure(figsize=(10, 6))
    borough_counts = data['BOROUGH'].value_counts()
    sns.barplot(x=borough_counts.index, y=borough_counts.values, palette='viridis', hue=borough_counts.index, dodge=False)
    plt.title('Number of Collisions by Borough')
    plt.xlabel('Borough')
    plt.ylabel('Number of Collisions')
    plt.xticks(rotation=45)
    plt.legend(title='Borough', loc='upper right')
    plt.savefig('collisions_by_borough.png')
    plt.close()

    # Visualization 2: Top Contributing Factors in Collisions
    plt.figure(figsize=(12, 8))
    factor_counts = data['CONTRIBUTING FACTOR VEHICLE 1'].value_counts().head(10)
    sns.barplot(x=factor_counts.values, y=factor_counts.index, palette='cubehelix', hue=factor_counts.index, dodge=False)
    plt.title('Top 10 Contributing Factors in Collisions')
    plt.xlabel('Number of Collisions')
    plt.ylabel('Contributing Factor')
    plt.tight_layout()
    plt.savefig('top_contributing_factors.png')
    plt.close()

    # Visualization 3: Collisions by Time of Day
    plt.figure(figsize=(12, 6))
    data['CRASH TIME'] = pd.to_datetime(data['CRASH TIME'], format='%H:%M', errors='coerce').dt.hour
    time_counts = data['CRASH TIME'].value_counts().sort_index()
    sns.lineplot(x=time_counts.index, y=time_counts.values, marker='o', sort=True, lw=2)
    plt.title('Collisions by Time of Day')
    plt.xlabel('Hour of the Day')
    plt.ylabel('Number of Collisions')
    plt.xticks(range(0, 24))
    plt.grid(True)
    plt.savefig('collisions_by_time.png')
    plt.close()

    # Visualization 4: Comparison of Injuries vs Fatalities
    plt.figure(figsize=(10, 6))
    injuries_killed = data[['NUMBER OF PERSONS INJURED', 'NUMBER OF PERSONS KILLED']].sum()
    injuries_killed.plot(kind='bar', color=['blue', 'red'])
    plt.title('Comparison of Injuries vs Fatalities')
    plt.xlabel('Type')
    plt.ylabel('Total Number')
    plt.xticks(rotation=0)
    plt.savefig('injuries_vs_fatalities.png')
    plt.close()

    # Visualization 5: Breakdown of Collisions by Vehicle Type
    plt.figure(figsize=(14, 8))
    vehicle_types = data['VEHICLE TYPE CODE 1'].value_counts().head(10)
    sns.barplot(x=vehicle_types.values, y=vehicle_types.index, color='skyblue')
    plt.title('Top 10 Vehicle Types Involved in Collisions')
    plt.xlabel('Number of Collisions')
    plt.ylabel('Vehicle Type')
    plt.tight_layout()
    plt.savefig('vehicle_type_collisions.png')
    plt.close()

    # Visualization 6: Heatmap of Collision Locations
    plt.figure(figsize=(10, 8))
    plt.hexbin(data['LONGITUDE'], data['LATITUDE'], gridsize=100, cmap='Blues', bins='log')
    plt.colorbar(label='log10(N)')
    plt.title('Heatmap of Collision Locations')
    plt.xlabel('Longitude')
    plt.ylabel('Latitude')
    plt.xlim(longitude_range)
    plt.ylim(latitude_range)
    plt.savefig('collision_location_heatmap.png')
    plt.close()

except Exception as e:
    logging.error(f"An error occurred during data visualization: {str(e)}")
finally:
    client.close()
    logging.info("MongoDB connection closed and visualization script completed")
