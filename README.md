# Real Estate Zip Code Scraper

## Setup

1. Create and activate a virtual environment:
```bash
python3 -m venv venv
source venv/bin/activate  # On macOS/Linux
# or
venv\Scripts\activate  # On Windows
```

2. Install dependencies:
```bash
pip install -r backend/requirements.txt
```

## Usage

Activate the virtual environment (if not already activated):
```bash
source venv/bin/activate  # On macOS/Linux
# or
venv\Scripts\activate  # On Windows
```

Run the CLI from the project root:
```bash
python -m backend.cli run
```

Or with custom config/output paths:
```bash
python -m backend.cli run --config-path backend/config.yaml --output-dir output
```

Or run directly:
```bash
cd backend
python cli.py run
```

## Project Structure

```
backend/
├── requirements.txt
├── config.yaml
├── utils.py
├── sources.py
├── scoring.py
├── finance.py
└── cli.py
```

