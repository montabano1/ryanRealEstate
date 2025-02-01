from firecrawl import FirecrawlApp
from pydantic import BaseModel, Field
from typing import Any, Optional, List
from datetime import datetime
import json
import os

class NestedModel1(BaseModel):
    address: str = Field(alias='location')  # Map location to address
    number_of_units: float = Field(default=0)  # Default to 0
    square_footage: str = Field(default='N/A')  # Default to N/A
    url: str = Field(default=None)
    contact_info: str = Field(default='N/A')  # Default to N/A
    price: str = Field(default='N/A')  # Default to N/A

class ExtractSchema(BaseModel):
    listings: List[NestedModel1]

def scrape_real_estate(api_key):
    app = FirecrawlApp(api_key=api_key)
    
    # Prepare the request payload
    payload = {
        'prompt': 'Ensure that the address is always included for each commercial real estate listing. Extract the number of units available, square footage, URL of the listing, contact information, and price.',
        'schema': ExtractSchema.model_json_schema(),
        'urls': [
            "https://loopnet.com/search/commercial-real-estate/new-york-ny/for-lease/*",
            "https://www.showcase.com/ny/new-york/commercial-real-estate/for-rent/*"
        ]
    }
    
    # Make the API request
    data = app.extract(
        [
            "https://loopnet.com/search/commercial-real-estate/new-york-ny/for-lease/*",
            "https://www.showcase.com/ny/new-york/commercial-real-estate/for-rent/*"
        ],
        {
            'prompt': 'Ensure that the address is always included for each commercial real estate listing. Extract the number of units available, square footage, URL of the listing, contact information, and price.',
            'schema': ExtractSchema.model_json_schema()
        }
    )
    
    # Save raw data
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    with open(f'data/raw_data_{timestamp}.json', 'w') as f:
        json.dump(data, f, indent=2)
    
    return data

if __name__ == "__main__":
    # Create data directory if it doesn't exist
    os.makedirs('data', exist_ok=True)
    scrape_real_estate()
