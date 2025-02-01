# NYC Commercial Real Estate Dashboard

A real-time dashboard for NYC commercial real estate listings using Firecrawl API.

## Project Structure
```
.
├── backend/             # Flask backend
│   ├── src/            # Python source files
│   ├── data/           # Data storage
│   ├── Dockerfile      # Docker configuration
│   └── requirements.txt # Python dependencies
└── frontend/           # Static frontend for GitHub Pages
    ├── static/         # Static assets
    │   ├── css/       # Stylesheets
    │   └── js/        # JavaScript files
    └── index.html     # Main HTML file
```

## Deployment Instructions

### Frontend (GitHub Pages)
1. Create a new repository on GitHub
2. Push the `frontend` directory contents to the repository
3. Enable GitHub Pages in repository settings
4. Update `API_BASE_URL` in `frontend/static/js/main.js` to point to your DigitalOcean app URL

### Backend (DigitalOcean)
1. Install DigitalOcean CLI: `brew install doctl`
2. Authenticate: `doctl auth init`
3. Create app:
   ```bash
   doctl apps create --spec backend/app.yaml
   ```
4. Add environment variables in DigitalOcean dashboard:
   - `FIRECRAWL_API_KEY`: Your Firecrawl API key

## Local Development
1. Frontend: Serve with any static file server
   ```bash
   cd frontend
   python -m http.server 5000
   ```
2. Backend: Run Flask development server
   ```bash
   cd backend
   pip install -r requirements.txt
   python src/app.py
   ```

## Environment Variables
Create a `.env` file in the backend directory:
```
FIRECRAWL_API_KEY=your_api_key_here
```
