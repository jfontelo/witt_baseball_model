=======
# Bobby Witt Jr Baseball Model 

This project is an exercise in data engineering and predictive analytics, building a predictive model for **Total Bases (TB) and Stolen Bases (SB)** for **Bobby Witt of the Kansas City Royals** to identify valuable betting opportunities.

## 📂 Project Structure

bobby_witt_model/
│── data/                    # Store raw & processed data (optional)
│── models/                  # Save trained models (optional)
│── scripts/                 # Store Python scripts for automation
│── notebooks/               # Jupyter notebooks for development
│── config.py                # Store database credentials (DO NOT SHARE)
│── database_setup.py        # Connects to PostgreSQL
│── data_collection.py       # Fetches Royals game data
│── model_training.py        # Trains Poisson model for TB & SB
│── predictions.py           # Runs model predictions
│── README.md                # Project documentation

## 📌 Setup Instructions

### 1️⃣ Install Dependencies
Run the following inside the project folder:
```bash
pip install -r requirements.txt
>>>>>>> b479aef (Initial commit)
