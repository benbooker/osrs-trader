# Setup Instructions

- Create a `.env` file in the project root directory with your database connection details:
```makefile
DB_NAME=prices_database
DB_USER=admin_user
DB_PASSWORD=SecurePassword7
DB_HOST=localhost
DB_PORT=5432
```

- Setup and activate a Python virtual environment:
```bash
python -m venv env
source env/bin/activate
pip install -r requirements.txt
```
