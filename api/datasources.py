import requests
from config import TABLEAU_SERVER, API_VERSION

def get_all_datasources(token, site_id):
    url = f"{TABLEAU_SERVER}/api/{API_VERSION}/sites/{site_id}/datasources"

    headers = {
        "X-Tableau-Auth": token,
        "Accept": "application/json"
    }

    response = requests.get(url, headers=headers)
    response.raise_for_status()

    return response.json().get("datasources", {}).get("datasource", [])
