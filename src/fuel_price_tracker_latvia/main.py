import requests
import re
import json
import pandas as pd
import os
import logging
import pytz
from datetime import datetime

# Configuration
RIGA_TZ = pytz.timezone('Europe/Riga')
DATA_DIR = "data"
LOG_DIR = "logs"
SITES = [
    {"url": "https://nozare.lv/figures/embed/bd5401f0-29a8-4c95-a802-6aa3e8fc2120", "filename": "95E_LV_cenas_nozarelv.csv"},
    {"url": "https://nozare.lv/figures/embed/f4a23a37-892d-4936-91e7-b447eb7f0bee", "filename": "DD_LV_cenas_nozarelv.csv"},
    {"url": "https://nozare.lv/figures/embed/0c72e48e-6c73-4962-bc21-9b45ab819efc/", "filename": "95E_Baltija_cenas_CircleK_nozarelv.csv"},
    {"url": "https://nozare.lv/figures/embed/dbd793e1-541e-42f1-8534-fb0a3a835655/", "filename": "DD_Baltija_cenas_CircleK_nozarelv.csv"}
]

# Ensure directories exist
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(DATA_DIR, exist_ok=True)

# Setup logging
log_file = os.path.join(LOG_DIR, "scrape_history.log")
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.FileHandler(log_file, encoding='utf-8'), logging.StreamHandler()]
)

def scrape_nozare_to_df(url):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        match = re.search(r'var ZA = (\{.*?\});', response.text, re.DOTALL)
        if not match:
            logging.error(f"Schema Change: Data variable not found at {url}")
            return None

        data = json.loads(match.group(1))
        series_list = data['nv']['datum']
        all_records = []

        for series in series_list:
            brand = series.get('key')
            for entry in series.get('values', []):
                all_records.append({
                    'Datums': entry.get('x'),
                    'Grupa': brand,
                    'Cena': entry.get('y')
                })

        df = pd.DataFrame(all_records)
        if df.empty: return None

        # Timezone shift: UTC -> Europe/Riga
        df['Datums'] = pd.to_datetime(df['Datums'], unit='ms', utc=True)
        df['Datums'] = df['Datums'].dt.tz_convert(RIGA_TZ).dt.tz_localize(None)
        
        # Pivot so brands are columns
        df_pivot = df.pivot(index='Datums', columns='Grupa', values='Cena')
        return df_pivot
    except Exception as e:
        logging.error(f"Request failed for {url}: {str(e)}")
        return None

def update_csv(site):
    file_path = os.path.join(DATA_DIR, site['filename'])
    df_new = scrape_nozare_to_df(site['url'])
    
    if df_new is None:
        logging.warning(f"No new data retrieved for {site['filename']}")
        return False

    if os.path.exists(file_path):
        # Load existing data with European settings
        df_old = pd.read_csv(file_path, sep=';', decimal=',', index_col='Datums', parse_dates=['Datums'])
        # Merge: newest data overwrites old data for the same timestamp
        df_combined = pd.concat([df_new, df_old])
        df_final = df_combined[~df_combined.index.duplicated(keep='first')]
    else:
        df_final = df_new

    # Sort descending (latest on top)
    df_final = df_final.sort_index(ascending=False)
    
    # Save with UTF-8-SIG for Excel compatibility
    df_final.to_csv(file_path, sep=';', decimal=',', encoding='utf-8-sig')
    
    latest_date = df_final.index[0]
    logging.info(f"Updated {site['filename']}. Newest record: {latest_date}")
    return True

if __name__ == "__main__":
    logging.info("Scrape session started.")
    for site in SITES:
        update_csv(site)
    logging.info("Scrape session finished.")