# Flask Aggregator

Flask Aggregator is a lightweight catalogue for scientific materials such as articles, dissertations and audio files.  It can scan a directory, extract text or audio transcripts and lets you browse the collection with flexible tagging and search.

## Features
- **Responsive UI**: works on mobile thanks to collapsible navigation and pagination.
- **Flexible tagging**: add any number of key/value tags with auto-completion.
- **Preview**: built-in viewer for text, images, PDFs and audio with thumbnails.
- **Faceted search**: filter by type, year and tags.  Active filters are shown as badges and can be cleared in one click.
- **Dark/Light theme**: switchable theme saved in the browser.

## Technologies
- Python, Flask, SQLAlchemy, SQLite
- Bootstrap based styling
- PyMuPDF, pytesseract, Whisper and other optional extractors

## Installation
```
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env  # set SCAN_ROOT inside
flask --app app.py init-db
python app.py
```
See `README_DEPLOY.md` for deployment tips including Docker and conda-pack.

## Usage
1. Open the app in a browser and use **Import** or **Upload** to add files.
2. Run scanning to extract text/metadata (heavy operations run in background threads).
3. Browse the catalogue, use search and filters.  Pagination avoids loading too many files at once.
4. Edit an entry to change metadata or manage tags.

## Contributing
Pull requests are welcome.  Please keep code style simple and run `python -m py_compile` on changed files before submitting.

## License
This project is released under the MIT license.
