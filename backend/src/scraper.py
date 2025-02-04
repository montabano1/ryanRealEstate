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

import logging

def scrape_real_estate(api_key):
    logging.info('Starting real estate scraping process')
    app = FirecrawlApp(api_key=api_key)
    
    # Prepare the request payload
    payload = {
        'prompt': 'Ensure that the address is always included for each commercial real estate listing. Extract the number of units available, square footage, URL of the listing, contact information, and price.',
        'schema': ExtractSchema.model_json_schema(),
        'urls': [
            "https://property.jll.com/search?tenureType=rent&propertyTypes=office&orderBy=desc&sortBy=dateModified",
        ]
    }
    
    # Log the schema
    schema = ExtractSchema.model_json_schema()
    logging.info(f'Using schema: {json.dumps(schema, indent=2)}')
    
    # Make the API request
    logging.info('Sending extraction request to Firecrawl API')
    try:
        # Log the request details
        request_urls = [
            "https://property.jll.com/search?tenureType=rent&propertyTypes=office&orderBy=desc&sortBy=dateModified"
        ]
        request_prompt = 'Extract all commercial real estate listings. For each listing, get the address, number of units available, square footage, URL, contact information, and price.'
        logging.info(f'Making request to URLs: {request_urls}')
        logging.info(f'Using prompt: {request_prompt}')
        
        try:
            data = app.extract(
                request_urls,
                {
                    'prompt': request_prompt,
                    'schema': ExtractSchema.model_json_schema()
                }
            )
        except Exception as api_error:
            logging.error(f'Error during Firecrawl API call: {str(api_error)}')
            logging.error(f'API error type: {type(api_error)}')
            logging.error(f'API error details: {api_error.__dict__ if hasattr(api_error, "__dict__") else "No details available"}')
            raise
        logging.info('Successfully received response from Firecrawl API')
        logging.info(f'Raw response data: {data}')
        
        # Log basic response info
        listings = data['data']['listings']
        logging.info(f'Found {len(listings)} listings')
        if listings:
            logging.info(f'Sample listing: {listings[0]}')
    except Exception as e:
        logging.error(f'Error during API request: {str(e)}')
        raise
    
    # Save raw data
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f'data/raw_data_{timestamp}.json'
    logging.info(f'Saving data to {filename}')
    try:
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2)
        logging.info('Data saved successfully')
    except Exception as e:
        logging.error(f'Error saving data: {str(e)}')
        raise
    
    return data

if __name__ == "__main__":
    # Create data directory if it doesn't exist
    os.makedirs('data', exist_ok=True)
    scrape_real_estate()
