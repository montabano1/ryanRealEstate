from flask import Flask, jsonify, render_template, request, Response
from flask_cors import CORS
from scraper import scrape_real_estate
import json
import os
import time
import threading
from datetime import datetime

# Global variables to track scraping progress
scraping_status = {
    'is_scraping': False,
    'current_data': None,
    'error': None
}

app = Flask(__name__)

# Configure CORS for GitHub Pages
CORS(app, resources={r"/api/*": {"origins": ["https://yourusername.github.io", "http://localhost:5000"]}}, supports_credentials=True)

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

# Ensure the data directory exists
os.makedirs('data', exist_ok=True)

# Ensure the data directory exists
os.makedirs('data', exist_ok=True)

@app.route('/')
def index():
    return render_template('index.html')

def generate_progress():
    """Generator function to yield real progress updates"""
    global scraping_status
    start_time = time.time()
    last_progress = 0
    
    while True:
        if scraping_status['error']:
            error_msg = f"Error: {scraping_status['error']}"
            yield f'data: {json.dumps({"progress": last_progress, "message": error_msg, "error": True})}\n\n'
            break
            
        elapsed_time = time.time() - start_time
        
        if elapsed_time < 2:
            message = "Initializing scraper..."
            progress = 5
        elif elapsed_time < 5:
            message = "Connecting to websites..."
            progress = 15
        elif elapsed_time < 10:
            message = "Searching for properties..."
            progress = 30
        elif elapsed_time < 20:
            message = "Fetching property listings..."
            progress = 50
        else:
            message = "Processing data..."
            progress = min(90, int(elapsed_time / 30 * 100))
        
        if scraping_status['current_data']:
            yield f'data: {json.dumps({"progress": 100, "message": "Complete!", "done": True})}\n\n'
            break
            
        if progress > last_progress:
            yield f'data: {json.dumps({"progress": progress, "message": message})}\n\n'
            last_progress = progress
            
        time.sleep(1)

@app.route('/api/progress')
def progress():
    return Response(
        generate_progress(),
        mimetype='text/event-stream',
        headers={
            'Cache-Control': 'no-cache',
            'Access-Control-Allow-Origin': '*',
            'Connection': 'keep-alive'
        }
    )

def run_scraper():
    """Run the scraper in a separate thread"""
    global scraping_status
    try:
        scraping_status['is_scraping'] = True
        data = scrape_real_estate(scraping_status.get('api_key'))
        
        # Save the data with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        os.makedirs('data', exist_ok=True)
        filename = f'data/raw_data_{timestamp}.json'
        
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2)
            
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
        data_files = [f for f in os.listdir(data_dir) if f.startswith('raw_data_')]
        if not data_files:
            return jsonify({
                'success': False,
                'error': 'No data available'
            })
        
        latest_file = max(data_files, key=lambda x: os.path.getmtime(os.path.join(data_dir, x)))
        with open(os.path.join(data_dir, latest_file), 'r') as f:
            data = json.load(f)
        
        # Get listings and convert location to address if needed
        listings = data.get('data', {}).get('listings', [])
        for listing in listings:
            if 'location' in listing and 'address' not in listing:
                listing['address'] = listing['location']
                del listing['location']
        
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
    app.run(debug=True, port=5001)
