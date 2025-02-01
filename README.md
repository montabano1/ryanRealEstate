# NYC Commercial Real Estate Scraper

This project uses Firecrawl to scrape commercial real estate listings in New York City and generate a static HTML report that can be hosted on GitHub Pages.

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Set up your Firecrawl API key in `src/scraper.py`

## Usage

1. Run the scraper:
```bash
python src/scraper.py
```

2. Generate the HTML report:
```bash
python src/generate_report.py
```

3. The report will be generated in the `docs` directory as `index.html`

## GitHub Pages Setup

1. Push this repository to GitHub
2. Go to repository settings
3. Under "GitHub Pages", select the "docs" folder as the source
4. Your report will be available at `https://[username].github.io/[repository-name]`

## Project Structure

- `src/`: Source code
  - `scraper.py`: Main scraping script
  - `generate_report.py`: Report generation script
- `templates/`: HTML templates
- `data/`: Raw scraped data
- `docs/`: Generated HTML report
