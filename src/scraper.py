from firecrawl import FirecrawlApp
from pydantic import BaseModel
from datetime import datetime
import json
import os

class NestedModel1(BaseModel):
    location: str
    suites_available: float = None
    square_footage: float = None
    price: str = None

class ExtractSchema(BaseModel):
    properties: list[NestedModel1]

def scrape_real_estate():
    app = FirecrawlApp(api_key='fc-5ac269b1d5a84653b7412b4b5dda8b61')
    
    data = app.extract([
        "https://loopnet.com/search/commercial-real-estate/new-york-ny/for-lease/*",
        "https://showcase.com/ny/new-york/commercial-real-estate/for-rent/?queries=%5Bobject%20Object%5D"
    ], {
        'prompt': 'Extract the location, suites available, square footage, and price for each commercial real estate listing in New York, NY. Ensure that the location is always included.',
        'schema': ExtractSchema.model_json_schema(),
    })
    
    # Save raw data
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    with open(f'data/raw_data_{timestamp}.json', 'w') as f:
        json.dump(data, f, indent=2)
    
    return data

if __name__ == "__main__":
    # Create data directory if it doesn't exist
    os.makedirs('data', exist_ok=True)
    scrape_real_estate()
