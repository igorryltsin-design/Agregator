# Flask Агрегатор — обновлённая версия

Добавлено:

## Запуск (Anaconda)
```bash
conda create -n flask_catalog python=3.10 -y
conda activate flask_catalog
pip install -r requirements.txt
cp .env.example .env
# укажите SCAN_ROOT в .env
flask --app app.py init-db
python app.py
## Quick notes

- Theme: the dark/light theme is controlled from Settings (Settings → "Тёмная тема"). The switch stores your choice in localStorage and is applied on each page load.

- Running locally:
	- Create a virtual environment: `python3 -m venv .venv`
	- Activate it: `source .venv/bin/activate`
	- Install requirements: `pip install -r requirements.txt`
	- Run: `python3 app.py`

