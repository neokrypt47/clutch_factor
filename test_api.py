import os
import requests
from dotenv import load_dotenv

# Load key from .env
load_dotenv()
API_KEY = os.getenv("RAPIDAPI_KEY")

url = "https://api-football-v1.p.rapidapi.com/v3/leagues"

headers = {
    "X-RapidAPI-Key": API_KEY,
    "X-RapidAPI-Host": "api-football-v1.p.rapidapi.com"
}

response = requests.get(url, headers=headers)

if response.status_code == 200:
    data = response.json()
    print("✅ API call successful. Sample data:")
    print(data['response'][0])
else:
    print("❌ API call failed:", response.status_code, response.text)
