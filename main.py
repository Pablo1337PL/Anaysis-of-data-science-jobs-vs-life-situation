import os
import requests
import sqlite3
import pandas as pd
import time
from dotenv import load_dotenv

load_dotenv()  # Ładuje zmienne środowiskowe z pliku .env

# --- KONFIGURACJA ---
APP_ID = os.getenv("APP_ID", "ADZUNA_APP_ID")
APP_KEY = os.getenv("APP_KEY", "ADZUNA_APP_KEY")

DB_NAME = "study_and_work_roi.db"
CSV_PATH = "education_costs.csv"

# Słownik: Kod kraju API -> Pełna nazwa z pliku Kaggle
COUNTRY_MAPPING = {
    'us': 'USA', 'gb': 'UK', 'ca': 'Canada', 'au': 'Australia', 'de': 'Germany',
    'jp': 'Japan', 'nl': 'Netherlands', 'sg': 'Singapore', 'fr': 'France', 
    'ch': 'Switzerland', 'se': 'Sweden', 'dk': 'Denmark', 'cn': 'China', 
    'kr': 'South Korea', 'ie': 'Ireland', 'nz': 'New Zealand', 'at': 'Austria', 
    'be': 'Belgium', 'hk': 'Hong Kong', 'pt': 'Portugal', 'il': 'Israel', 
    'tw': 'Taiwan', 'cz': 'Czech Republic', 'in': 'India', 'pl': 'Poland', 
    'my': 'Malaysia', 'es': 'Spain', 'it': 'Italy', 'fi': 'Finland', 'no': 'Norway', 
    'br': 'Brazil', 'tr': 'Turkey', 'ru': 'Russia', 'mx': 'Mexico', 'gr': 'Greece', 
    'th': 'Thailand', 'ae': 'UAE', 'za': 'South Africa', 'eg': 'Egypt', 'ar': 'Argentina', 
    'id': 'Indonesia', 'sa': 'Saudi Arabia', 'ng': 'Nigeria', 'vn': 'Vietnam', 
    'hu': 'Hungary', 'is': 'Iceland', 'co': 'Colombia', 'ro': 'Romania', 'lu': 'Luxembourg', 
    'tn': 'Tunisia', 'cy': 'Cyprus', 'hr': 'Croatia', 'do': 'Dominican Republic', 
    'ma': 'Morocco', 'pe': 'Peru', 'ec': 'Ecuador', 'lb': 'Lebanon', 'bh': 'Bahrain', 
    'uy': 'Uruguay', 'bg': 'Bulgaria', 'gh': 'Ghana', 'dz': 'Algeria', 'pa': 'Panama', 
    'bd': 'Bangladesh', 'kw': 'Kuwait', 'ua': 'Ukraine', 'si': 'Slovenia', 'rs': 'Serbia', 
    'ir': 'Iran', 'uz': 'Uzbekistan', 'sv': 'El Salvador'
}

COUNTRIES = list(COUNTRY_MAPPING.keys())

REVERSE_MAPPING = {v: k for k, v in COUNTRY_MAPPING.items()}


MAX_PAGES_PER_COUNTRY = 5  # Zwiększ tę liczbę, aby pobrać jeszcze więcej ofert (np. 10 lub 20)

def fetch_jobs_by_city(country_code, city):
    """Pobiera oferty dla konkretnego miasta w danym kraju."""
    all_city_jobs = []
    print(f"   -> Pobieranie ofert dla: {city} ({country_code.upper()})...")
    
    # Dla miast pobieramy zazwyczaj 1-2 strony, by nie przekroczyć limitów API
    for page in range(1, 3): 
        url = f"https://api.adzuna.com/v1/api/jobs/{country_code}/search/{page}"
        params = {
            'app_id': APP_ID,
            'app_key': APP_KEY,
            'results_per_page': 50,
            'what': 'data',
            'where': city,  # KLUCZOWA ZMIANA: filtrowanie po mieście
            'content-type': 'application/json'
        }
        
        try:
            response = requests.get(url, params=params)
            if response.status_code == 404: break
            response.raise_for_status()
            results = response.json().get('results', [])
            
            if not results: break
            all_city_jobs.extend(results)
            time.sleep(1) # Rate limiting
            
        except Exception as e:
            print(f"      Błąd: {e}")
            break
            
    return all_city_jobs

def process_api_jobs(raw_jobs, country_name, city_name):
    """Pakuje dane, dodając nazwy z CSV dla łatwego JOINa."""
    processed = []
    for job in raw_jobs:
        processed.append({
            'job_id': str(job.get('id')),
            'country_name': country_name, # Nazwa identyczna jak w CSV
            'city_name': city_name,   # Nazwa identyczna jak w CSV
            'title': job.get('title'),
            'company': job.get('company', {}).get('display_name'),
            'salary_min': job.get('salary_min'),
            'salary_max': job.get('salary_max'),
            'salary_currency': job.get('salary_currency_code', 'unknown'),  # Waluta
            #'salary_period': job.get('salary_period'),# 'yearly'),  # Okres (yearly, monthly, etc.)
            'latitude': job.get('latitude'),
            'longitude': job.get('longitude'),
            'contract_time': job.get('contract_time', 'unknown'),
            'category': job.get('category', {}).get('label'),
            'created_at': job.get('created'),
            'url': job.get('redirect_url')
        })
    return processed

if __name__ == "__main__":
    if not os.path.exists(CSV_PATH):
        print(f"BŁĄD: Brak pliku {CSV_PATH}")
    else:
        # 1. Wczytaj miasta z CSV
        df_edu = pd.read_csv(CSV_PATH)
        # Pobieramy unikalne pary Kraj-Miasto (tylko dla krajów wspieranych przez Adzunę)
        relevant_locations = df_edu[df_edu['Country'].isin(REVERSE_MAPPING.keys())][['Country', 'City']].drop_duplicates()
        
        relevant_locations = relevant_locations.sample(frac=1).reset_index(drop=True)
        
        all_jobs_data = []

        # 2. Iteruj po miastach z pliku
        print(f"Znaleziono {len(relevant_locations)} unikalnych lokalizacji do sprawdzenia.")
        for _, row in relevant_locations.iterrows():
            print(_, row['Country'], row['City'])

            c_name = row['Country']
            city = row['City']
            c_code = REVERSE_MAPPING.get(c_name)
            
            if c_code:
                raw_data = fetch_jobs_by_city(c_code, city)
                processed = process_api_jobs(raw_data, c_name, city)
                all_jobs_data.extend(processed)

        # 3. Zapisz do bazy
        if all_jobs_data:
            conn = sqlite3.connect(DB_NAME)
            pd.DataFrame(all_jobs_data).to_sql('fact_job_postings', conn, if_exists='replace', index=False)
            df_edu.to_sql('fact_education_cost', conn, if_exists='replace', index=False)
            conn.close()
            print(f"\n[SUKCES] Pobrano łącznie {len(all_jobs_data)} ofert pasujących do miast z CSV.")