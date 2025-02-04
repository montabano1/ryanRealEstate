from flask import Flask, jsonify, request, Response
from flask_cors import CORS
from scraper import scrape_real_estate
import json
import os
import time
import threading
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Global variables to track scraping status
scraping_status = {
    'is_scraping': False,
    'current_data': None,
    'error': None
}

app = Flask(__name__)

# Configure CORS for GitHub Pages and local development
CORS(app, resources={r"/api/*": {"origins": [
    "https://montabano1.github.io",
    "http://localhost:5000",
    "http://127.0.0.1:5000",
    "http://localhost:8000",
    "http://127.0.0.1:8000"
]}}, supports_credentials=True)

# Ensure the data directory exists
os.makedirs('data', exist_ok=True)

@app.route('/')
def index():
    return jsonify({'status': 'API is running'})



def run_scraper():
    """Run the scraper in a separate thread"""
    global scraping_status
    try:
        logging.info('Starting scraper with API key: %s', scraping_status.get('api_key')[:8] + '...')
        scraping_status['is_scraping'] = True
        data = scrape_real_estate(scraping_status.get('api_key'))
        
        # Save the data with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        os.makedirs('data', exist_ok=True)
        filename = f'data/raw_data_{timestamp}.json'
        
        logging.info('Saving data to %s', filename)
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2)
        logging.info('Data saved successfully')
            
        scraping_status['current_data'] = True
    except Exception as e:
        scraping_status['error'] = str(e)
    finally:
        scraping_status['is_scraping'] = False

@app.route('/api/generate-report', methods=['POST'])
def generate_report():
    global scraping_status
    try:
        # Check content type
        if not request.is_json:
            return jsonify({
                'success': False,
                'error': f'Content-Type must be application/json. Got {request.content_type}'
            }), 415

        # Get request data
        data = request.get_json()
        if not data:
            return jsonify({
                'success': False,
                'error': 'No JSON data received'
            }), 400

        # Get API key
        api_key = data.get('api_key')
        if not api_key:
            return jsonify({
                'success': False,
                'error': 'API key is required'
            }), 400

        # Reset status first
        scraping_status['is_scraping'] = False
        scraping_status['current_data'] = None
        scraping_status['error'] = None
        scraping_status['api_key'] = api_key
        
        # Start scraping in a separate thread
        thread = threading.Thread(target=run_scraper)
        thread.start()
        
        return jsonify({
            'success': True,
            'message': 'Scraping started'
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/status')
def get_status():
    global scraping_status
    try:
        if scraping_status['error']:
            return jsonify({
                'success': False,
                'error': scraping_status['error']
            })
        elif scraping_status['is_scraping']:
            return jsonify({
                'success': False,
                'error': 'Data not ready'
            })
        elif scraping_status['current_data']:
            return jsonify({
                'success': True,
                'data': scraping_status['current_data']
            })
        else:
            return jsonify({
                'success': False,
                'error': 'No data available'
            })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        })

@app.route('/api/latest-data', methods=['GET'])
def get_latest_data():
    try:
        # Get the latest data file
        data_dir = 'data'
        logging.info(f'Looking for data files in {data_dir}')
        data_files = [f for f in os.listdir(data_dir) if f.startswith('raw_data_')]
        logging.info(f'Found data files: {data_files}')
        
        if not data_files:
            logging.warning('No data files found')
            return jsonify({
                'success': False,
                'error': 'No data available'
            })
        
        latest_file = max(data_files, key=lambda x: os.path.getmtime(os.path.join(data_dir, x)))
        logging.info(f'Loading latest file: {latest_file}')
        
        with open(os.path.join(data_dir, latest_file), 'r') as f:
            data = json.load(f)
            logging.info(f'Loaded data structure: {list(data.keys())}')
        
        # Get listings and convert location to address if needed
        listings = data.get('data', {}).get('listings', [])
        logging.info(f'Found {len(listings)} listings in data')
        
        for listing in listings:
            if 'location' in listing and 'address' not in listing:
                listing['address'] = listing['location']
                del listing['location']
                
        logging.info(f'Processed {len(listings)} listings for response')
        if listings:
            logging.info(f'Sample listing: {listings[0]}')
        
        return jsonify({
            'success': True,
            'data': {
                'properties': listings
            }
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', type=int, default=5001)
    args = parser.parse_args()
    app.run(debug=True, port=args.port)
