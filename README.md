📘 Energy Analytics Regression Prototype
A unified, production‑ready pipeline for AESO, IESO, and User‑Uploaded energy generation datasets, with:

Automated data ingestion

Weather integration (Open‑Meteo)

Linear + Polynomial regression

Best‑model selection

Plot generation

FastAPI backend

Config‑driven architecture

Day‑to‑day caching

This project is designed for Azure deployment and provides a clean, modular structure for future expansion.

🚀 Features
✅ Three Data Sources
AESO (Alberta) — CSV generation data

IESO (Ontario) — XML generation data

User Upload — custom CSVs

✅ Weather Integration
Uses Open‑Meteo Archive API to fetch:

Temperature

Humidity

Cloud cover

Wind speed/gust/direction

Radiation variables

Pressure

✅ Regression Engine (scikit‑learn)
Runs both:

Linear Regression

Polynomial Regression (degree 2 + interactions + lag features)

Automatically selects the best model based on R².

✅ Plot Generation
Scatter (Actual vs Predicted)

Time‑series (Test set)

Saved to output/plots/

✅ FastAPI Backend
Endpoints:

Code
/run-aeso
/run-ieso
/run-upload (coming soon)
Returns:

R²

Equations

Plot paths

Best model

Model-ready CSVs

✅ Config‑Driven
All settings stored in:

Code
config.yaml
✅ Day‑to‑Day Caching
Metadata stored in:

Code
output/metadata.json
Prevents unnecessary reprocessing.

📁 Project Structure
Code
capstone_prototype/
│
├── app/
│   ├── main.py
│   └── routers/
│       ├── aeso.py
│       ├── ieso.py
│       └── upload.py
│
├── services/
│   ├── universal_pipeline.py
│   ├── metadata_manager.py
│   ├── error_handler.py
│   └── storage.py
│
├── adapters/
│   ├── aeso_adapter.py
│   ├── ieso_adapter.py
│   └── user_adapter.py
│
├── pipelines/
│   ├── aeso_pipeline.py
│   ├── ieso_pipeline.py
│   └── user_pipeline.py
│
├── weather/
│   └── weather_fetcher.py
│
├── models/
│   └── regression_engine.py
│
├── output/
│   ├── aeso/
│   ├── ieso/
│   ├── user/
│   └── metadata.json
│
├── logs/
│   └── pipeline.log
│
├── config.yaml
├── requirements.txt
└── README.md
🛠 Installation
1. Clone the project
Code
git clone <your-repo-url>
cd capstone_prototype
2. Create a virtual environment
Code
python -m venv venv
source venv/bin/activate   # Mac/Linux
venv\Scripts\activate      # Windows
3. Install dependencies
Code
pip install -r requirements.txt
▶️ Running the API
Start the FastAPI server:

Code
uvicorn app.main:app --reload
Open your browser:

Code
http://127.0.0.1:8000/docs
You’ll see interactive API documentation.

📊 Running a Pipeline
IESO:
Code
GET /run-ieso
AESO:
Code
GET /run-aeso
Response includes:
Linear model results

Polynomial model results

Best model

Plot paths

Equations

R²

🧠 How It Works
1. Universal Pipeline
Handles:

Market selection

Metadata caching

Weather fetching

Model-ready CSV creation

Regression execution

2. Regression Engine
Runs:

Linear Regression

Polynomial Regression

Lag features

Best-model selection

Plot generation

3. FastAPI Layer
Returns results in a UI‑friendly JSON format.

📦 Future Enhancements
User-uploaded model pipeline

Model serialization (pickle)

Hyperparameter tuning

Front-end dashboard

Azure Blob Storage integration