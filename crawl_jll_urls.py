import asyncio
import json
from datetime import datetime
from bs4 import BeautifulSoup
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode, MemoryAdaptiveDispatcher, CrawlerMonitor, DisplayMode

async def extract_property_urls():
    start_time = datetime.now()
    
    def log_time(step_name):
        elapsed = datetime.now() - start_time
        print(f"\n[{step_name}] Time elapsed: {elapsed}")
    
    browser_config = BrowserConfig(
        headless=False,
        verbose=True
    )
    
    # Wait for select element and property cards
    base_wait = """js:() => {
        const select = document.getElementById("q_type_use_offset_eq_any");
        const cards = document.querySelectorAll('.property-card');
        return select !== null || cards.length > 0;
    }"""
    
    js_wait = """
        await new Promise(r => setTimeout(r, 3000));
        """
    
    js_next_page = """
        const selector = 'li.flex.items-center > button > svg.h-6.text-jllRed';
        const button = document.querySelector(selector).closest('button');
        if (button) {
            console.log('found button')
            button.click()
        } else {
            console.log('no button')
            return False    
        };
        await new Promise(r => setTimeout(r, 5000));
        """
    

    
    async with AsyncWebCrawler(config=browser_config) as crawler:
        try:
            print("\nStarting property URL extraction...")
            
            session_id = "monte"

            # Step 2: Extract URLs using BeautifulSoup
            print("Extracting property URLs...")
            all_property_urls = set()  # Using a set to avoid duplicates
            last_page_urls = set()
            
            config_first = CrawlerRunConfig(
                session_id=session_id,
                js_code=js_wait,
                css_selector='div.grid',
                cache_mode=CacheMode.BYPASS
            )
            result1 = await crawler.arun(
                url='https://property.jll.com/search?tenureType=rent&propertyTypes=office&orderBy=desc&sortBy=dateModified',
                config=config_first,
                session_id=session_id
            )
            # Get URLs from first page

            property_links = result1.links["internal"]
            current_page_urls = {link['href'] for link in property_links}  # No need to add base URL, it's already there
            all_property_urls.update(current_page_urls)
            print(f"Found {len(current_page_urls)} property URLs on page 1")
            
            page_num = 2
            for page in range(1):
                # Store the current page URLs to compare with next page
                last_page_urls = current_page_urls
                
                config_next = CrawlerRunConfig(
                    session_id=session_id,
                    js_code=js_next_page,
                    js_only=True,      
                    cache_mode=CacheMode.BYPASS
                )
                result2 = await crawler.arun(
                    url='https://property.jll.com/search?tenureType=rent&propertyTypes=office&orderBy=desc&sortBy=dateModified',
                    config=config_next,
                    session_id=session_id
                )
                
                property_links = result2.links["internal"]
                current_page_urls = {link['href'] for link in property_links}
                
                # If we got the same URLs as the last page, we've reached the end
                if current_page_urls == last_page_urls:
                    print("No new URLs found - reached end of pagination")
                    break
                
                all_property_urls.update(current_page_urls)
                print(f"Found {len(current_page_urls)} property URLs on page {page_num}")
                print(f"Total unique URLs so far: {len(all_property_urls)}")
                page_num += 1
            
            # Save URLs to a JSON file
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        
            # Process all URLs using arun_many with memory adaptive dispatcher
            all_property_details = []
            urls_to_process = list(all_property_urls)
            
            log_time("URL Collection Complete")
            
            # Create a run config for property details extraction
            run_config = CrawlerRunConfig(
                cache_mode=CacheMode.BYPASS,
                stream=False  # Get all results at once
            )
            
            # Set up the memory adaptive dispatcher
            dispatcher = MemoryAdaptiveDispatcher(
                memory_threshold_percent=70.0,
                check_interval=1.0,
                max_session_permit=10,
                monitor=CrawlerMonitor(
                    display_mode=DisplayMode.DETAILED
                )
            )
            
            print("\nStarting batch processing of URLs...")
            
            log_time("Iframe Collection Complete")
            
            # Get all results at once using memory adaptive dispatcher
            results = await crawler.arun_many(
                urls=all_property_urls,
                config=run_config,
                dispatcher=dispatcher
            )
            
            # Process all results after completion
            for result in results:
                if result.success and result.html:
                    soup = BeautifulSoup(result.html, 'html.parser')
                    print(soup.select_one('h1').text)
                    
                    
                    # Extract unit details from table
                    # units = []
                    # for row in soup.select('.js-lease-space-row-toggle.spaces'):
                    #     cells = row.find_all(['th', 'td'])
                    #     if len(cells) >= 5:
                    #         unit = {
                    #             "property_name": property_name,
                    #             "address": address,
                    #             "location": location,
                    #             "listing_url": result.url,
                    #             "error": False,
                    #             "floor_suite": cells[0].text.strip(),
                    #             "space_available": cells[2].text.strip(),
                    #             "price": cells[3].text.strip()
                    #         }
                    #         units.append(unit)
                    
                    # all_property_details.extend(units)
                else:
                    print(f"Failed to process {result.url}: {result.error_message if hasattr(result, 'error_message') else 'Unknown error'}")
            
            # Save all property details to a JSON file
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"lee_properties_{timestamp}.json"
            
            with open(output_file, 'w') as f:
                json.dump(all_property_details, f, indent=2)
            
            print(f"\nExtracted {len(all_property_details)} total units from {len(urls_to_process)} properties")
            print(f"Results saved to {output_file}")
            
            total_time = datetime.now() - start_time
            print(f"\n=== Final Statistics ===")
            print(f"Total Properties Found: {len(urls_to_process)}")
            print(f"Total Iframe URLs: {len(iframe_urls)}")
            print(f"Total Units Extracted: {len(all_property_details)}")
            print(f"Total Time: {total_time}")
            print(f"Average Time per Property: {total_time / len(urls_to_process) if urls_to_process else 0}")
            
            return all_property_details
            
                
        except Exception as e:
            print(f"Error during extraction: {e}")

async def extract_property_details(url):
    """Extract property details from a Lee Associates property page"""
    print(f"Extracting details from {url}")
    
    browser_cfg = BrowserConfig(headless=False, verbose=True)
    run_config = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        wait_for="css:.pdt-header1, .pdt-header2, .js-lease-space-row-toggle"
    )
    
    units = []
    try:
        async with AsyncWebCrawler(config=browser_cfg) as crawler:
            # First get the iframe URL
            iframe_url = await get_iframe_url(url)
            if not iframe_url:
                print("Could not find iframe URL")
                return None
                
            iframe_url = iframe_url + '&tab=spaces'
            print(f"Found iframe: {iframe_url}")
            
            # Now get the content from the iframe
            result = await crawler.arun(url=iframe_url, config=run_config)
            if not result.success:
                print("Failed to load iframe content")
                return None
                
            soup = BeautifulSoup(result.html, 'html.parser')
            
            # Extract property name
            name_elem = soup.select_one('.pdt-header1 h1')
            property_name = name_elem.text.strip() if name_elem else ""
            
            # Extract address and location
            addr_elem = soup.select_one('.pdt-header2 h2')
            if addr_elem:
                addr_parts = addr_elem.text.strip().split('|')
                address = addr_parts[0].strip()
                location = addr_parts[1].strip() if len(addr_parts) > 1 else ""
            else:
                address = property_name  # If no separate address, use property name
                location = ""
                
            # Extract unit details from table
            for row in soup.select('.js-lease-space-row-toggle.spaces'):
                cells = row.find_all(['th', 'td'])
                if len(cells) >= 5:  # Ensure we have enough cells
                    unit = {
                        "property_name": property_name,
                        "address": address,
                        "location": location,
                        "listing_url": url,
                        "error": False,
                        "floor_suite": cells[0].text.strip(),
                        "space_available": cells[2].text.strip(),
                        "price": cells[3].text.strip()
                    }
                    units.append(unit)
            
            print(f"Found {len(units)} units")
            return units
                
    except Exception as e:
        print(f"Error extracting property details: {e}")
        return None

async def test_single_property():
    """Test property details extraction"""
    test_url = "https://www.lee-associates.com/properties/?propertyId=1115269-lease&address=2250-S-Barrington-Ave&officeId=2429"
    print(f"Testing URL: {test_url}")
    
    units = await extract_property_details(test_url)
    if units:
        print("\nExtracted Units:")
        for unit in units:
            print(json.dumps(unit, indent=2))
    else:
        print("No units found")
    
    return units

if __name__ == "__main__":
    # Run the full extraction
    # asyncio.run(test_single_property())
    asyncio.run(extract_property_urls())
