import os
import requests
import sqlite3
import pandas as pd
import time
from dotenv import load_dotenv

load_dotenv()  # Ładuje zmienne środowiskowe z pliku .env

# --- KONFIGURACJA ---
APP_ID = os.getenv("ADZUNA_APP_ID", "APP_ID")
APP_KEY = os.getenv("ADZUNA_APP_KEY", "APP_KEY")
DB_NAME = "study_and_work_roi.db"


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



MAX_PAGES_PER_COUNTRY = 5  # Zwiększ tę liczbę, aby pobrać jeszcze więcej ofert (np. 10 lub 20)

def fetch_max_jobs(country):
    """Pobiera wiele stron wyników dla danego kraju."""
    all_country_jobs = []
    print(f"\n-> Rozpoczynam pobieranie ofert dla: {country.upper()}")
    
    for page in range(1, MAX_PAGES_PER_COUNTRY + 1):
        url = f"https://api.adzuna.com/v1/api/jobs/{country}/search/{page}"
        params = {
            'app_id': APP_ID,
            'app_key': APP_KEY,
            'results_per_page': 50,  # Max na jedno zapytanie to 50
            'what': 'data',
            'content-type': 'application/json'
        }
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            results = response.json().get('results', [])
            
            if not results:
                print(f"   Brak więcej wyników na stronie {page}. Kończę pobieranie dla {country}.")
                break
                
            all_country_jobs.extend(results)
            print(f"   Pobrano stronę {page} ({len(results)} ofert)")
            time.sleep(1) # Opóźnienie, aby nie zablokowali nam API (Rate Limiting)
            
        except requests.exceptions.RequestException as e:
            print(f"   Błąd API na stronie {page}: {e}")
            break
            
    return all_country_jobs

def process_api_jobs(raw_jobs, country_code):
    """Wyciąga MAKSIMUM informacji z JSONa z ofertą."""
    processed = []
    for job in raw_jobs:
        processed.append({
            'job_id': str(job.get('id')),
            'country_code': country_code.upper(),
            'title': job.get('title'),
            'company_name': job.get('company', {}).get('display_name'),
            'location_name': job.get('location', {}).get('display_name'),
            'latitude': job.get('latitude'),
            'longitude': job.get('longitude'),
            'salary_min': job.get('salary_min'),
            'salary_max': job.get('salary_max'),
            'contract_type': job.get('contract_type', 'unknown'),
            'contract_time': job.get('contract_time', 'unknown'),
            'category': job.get('category', {}).get('label'),
            'description': job.get('description'),
            'url': job.get('redirect_url'),
            'created_at': job.get('created')
        })
    return processed

def create_database(df_jobs, csv_path):
    """Tworzy bazę i dwie tabele faktów."""
    conn = sqlite3.connect(DB_NAME)
    
    # 1. TABELA FAKTÓW 1: Oferty Pracy (API)
    df_jobs.to_sql('fact_job_postings', conn, if_exists='replace', index=False)
    print(f"[OK] Zapisano {len(df_jobs)} ofert pracy do tabeli 'fact_job_postings'.")
    
    # 2. TABELA FAKTÓW 2: Koszty Edukacji (Kaggle CSV)
    try:
        # Wczytujemy plik CSV pobrany z Kaggle
        df_education = pd.read_csv(csv_path)
        
        # Opcjonalnie: ujednolicenie nazw kolumn (zastąpienie spacji podkreślnikami)
        df_education.columns = df_education.columns.str.replace(' ', '_').str.lower()
        
        df_education.to_sql('fact_education_cost', conn, if_exists='replace', index=False)
        print(f"[OK] Zapisano dane z Kaggle do tabeli 'fact_education_cost'.")
    except FileNotFoundError:
        print(f"[BŁĄD] Nie znaleziono pliku '{csv_path}'. Pobierz go z Kaggle i wrzuć do folderu ze skryptem.")
    
    conn.close()

if __name__ == "__main__":
    if APP_ID == "APP_ID":
        print("BŁĄD: Zmień APP_ID i APP_KEY na swoje dane z Adzuny!")
    else:
        all_jobs_data = []
        
        # Krok 1: Pobieranie z API
        for c in COUNTRIES:
            raw_data = fetch_max_jobs(c)
            processed_data = process_api_jobs(raw_data, c)
            all_jobs_data.extend(processed_data)
            
        # Krok 2: Konwersja do DataFrame
        df_all_jobs = pd.DataFrame(all_jobs_data)
        
        # Krok 3: Zapis bazy (Integracja API + Plik CSV)
        if not df_all_jobs.empty:
            # Zakładam, że plik z Kaggle nazywa się education_costs.csv
            create_database(df_all_jobs, csv_path='education_costs.csv')
        else:
            print("Brak danych do zapisania.")