import json
from datetime import datetime
import os
from jinja2 import Environment, FileSystemLoader

def generate_report(data):
    # Setup Jinja2 environment
    env = Environment(loader=FileSystemLoader('templates'))
    template = env.get_template('report_template.html')
    
    # Prepare data for the template
    properties = data.get('data', {}).get('properties', []) if isinstance(data, dict) else []
    context = {
        'properties': properties,
        'generated_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'total_properties': len(properties)
    }
    
    # Generate HTML
    html_content = template.render(context)
    
    # Save the report
    os.makedirs('docs', exist_ok=True)
    with open('docs/index.html', 'w') as f:
        f.write(html_content)

if __name__ == "__main__":
    # Read the latest data file
    data_files = sorted([f for f in os.listdir('data') if f.startswith('raw_data_')])
    if not data_files:
        print("No data files found!")
        exit(1)
        
    latest_file = data_files[-1]
    with open(f'data/{latest_file}', 'r') as f:
        data = json.load(f)
    
    generate_report(data)
