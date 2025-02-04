import asyncio
import json
import os
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode
from crawl4ai.extraction_strategy import LLMExtractionStrategy
from pydantic import BaseModel, Field

# Load environment variables
load_dotenv(dotenv_path=Path('.env'))

class PropertyUnit(BaseModel):
    property_name: str = Field(..., description="The name of the property in uppercase")
    floor_suite: str = Field(..., description="Floor and suite number")
    address: str = Field(..., description="Street address")
    location: str = Field(..., description="City, State, and ZIP code")
    price: str = Field(..., description="Price per SF or contact message")
    space_available: str = Field(..., description="Available space in SF")
    listing_url: str = Field(..., description="Full URL of the property listing")

async def extract_unit_details(crawler, url, property_info):
    print(f"\nExtracting unit details from {url}")
    
    unit_extraction_strategy = LLMExtractionStrategy(
        provider="openai/gpt-4o-mini",
        api_token=os.getenv('OPENAI_API_KEY'),
        schema={"type": "array", "items": {"type": "object", "properties": {
            "floor_suite": {"type": "string"},
            "space_available": {"type": "string"}
        }}},
        extraction_type="schema",
        instruction="""
        Extract all available unit details from the data grid on the page.
        For each row in the grid, provide:
        - floor_suite: The value from the "Floor" column (e.g. "3rd Floor, Suite 300")
        - space_available: The value from the "Size" column (e.g. "14,740 SF")
        Return as a list of objects with these two fields.
        """,
        model_kwargs={"stream": False}
    )

    run_config = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        extraction_strategy=unit_extraction_strategy,
        js_code=[
            """
            async function waitForContent() {
                await new Promise(r => setTimeout(r, 5000));
                return true;
            }
            return await waitForContent();
            """
        ]
    )

    result = await crawler.arun(
        url=url,
        config=run_config,
        magic=True
    )

    units = []
    if result.extracted_content:
        try:
            content = json.loads(result.extracted_content) if isinstance(result.extracted_content, str) else result.extracted_content
            if isinstance(content, list):
                for unit in content:
                    unit_obj = property_info.copy()
                    unit_obj["floor_suite"] = unit["floor_suite"]
                    unit_obj["space_available"] = unit["space_available"]
                    units.append(unit_obj)
            print(f"Extracted {len(units)} units")
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON content: {e}")
    
    return units

async def extract_properties_paginated():
    browser_config = BrowserConfig(
        headless=False,
        verbose=True
    )

    property_extraction_strategy = LLMExtractionStrategy(
        provider="openai/gpt-4o-mini",
        api_token=os.getenv('OPENAI_API_KEY'),
        schema={"type": "array", "items": {"type": "object", "properties": {
            "property_name": {"type": "string"},
            "address": {"type": "string"},
            "location": {"type": "string"},
            "price": {"type": "string"},
            "listing_url": {"type": "string"}
        }}},
        extraction_type="schema",
        instruction="""
        Extract property listing details from the HTML content. For each property listing, provide:
        - Property name exactly as shown (in uppercase)
        - Complete street address
        - City, State, and ZIP exactly as shown
        - Price information (either price per SF or contact message)
        - Full listing URL from the href attribute in the property card link
        
        Keep all numerical values and formatting exactly as shown in the original text.
        Include the full listing URL by combining 'https://property.jll.com' with the href.
        Return the data in JSON format matching the provided schema.
        """,
        model_kwargs={"stream": False}
    )

    all_units = []
    MAX_PAGES = 100  # Safety limit to prevent infinite loops
    
    run_config = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        extraction_strategy=property_extraction_strategy,
        js_code=[
            """
            async function waitForContent() {
                console.log('Waiting for page to load...');
                await new Promise(r => setTimeout(r, 5000));
                return true;
            }
            return await waitForContent();
            """
        ]
    )
    
    async with AsyncWebCrawler(config=browser_config) as crawler:
        try:
            print("\nStarting paginated property extraction...")
            current_page = 1
            base_url = "https://property.jll.com/search?tenureType=rent&propertyType=office"
            
            while current_page <= MAX_PAGES:
                current_url = f"{base_url}&page={current_page}" if current_page > 1 else base_url
                print(f"\nProcessing page {current_page}...")
                
                result = await crawler.arun(
                    url=current_url,
                    config=run_config,
                    magic=True
                )
                
                if result.extracted_content:
                    try:
                        content = json.loads(result.extracted_content) if isinstance(result.extracted_content, str) else result.extracted_content
                        
                        if isinstance(content, list):
                            if len(content) == 0:
                                print("No properties found on this page, assuming end of listing")
                                break
                                
                            # Process properties concurrently in batches
                            batch_size = 10  # Process 10 properties at a time
                            for i in range(0, len(content), batch_size):
                                batch = content[i:i + batch_size]
                                tasks = [
                                    extract_unit_details(crawler, prop["listing_url"], prop)
                                    for prop in batch
                                ]
                                batch_results = await asyncio.gather(*tasks)
                                for units in batch_results:
                                    all_units.extend(units)
                                
                                if i + batch_size < len(content):
                                    await asyncio.sleep(5)  # Brief delay between batches
                            
                            print(f"Processed {len(content)} properties from page {current_page}")
                            current_page += 1
                            await asyncio.sleep(5)  # Delay between pages
                            
                        elif isinstance(content, dict):
                            units = await extract_unit_details(crawler, content["listing_url"], content)
                            all_units.extend(units)
                            print(f"Processed 1 property from page {current_page}")
                            current_page += 1
                            await asyncio.sleep(5)
                        
                    except json.JSONDecodeError as e:
                        print(f"Error parsing JSON content: {e}")
                        break
                else:
                    print("No content extracted from page")
                    break
                
            # Save all extracted units to a JSON file
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"jll_units_{timestamp}.json"
            
            with open(output_file, 'w') as f:
                json.dump(all_units, f, indent=2)
            
            print(f"\nExtracted {len(all_units)} total units")
            print(f"Results saved to {output_file}")
            
        except Exception as e:
            print(f"Error during extraction: {e}")

if __name__ == "__main__":
    asyncio.run(extract_properties_paginated())
