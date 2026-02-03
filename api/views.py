import requests
from config import TABLEAU_SERVER, API_VERSION

def get_views_for_workbook(token, site_id, workbook_id):
    url = f"{TABLEAU_SERVER}/api/{API_VERSION}/sites/{site_id}/workbooks/{workbook_id}/views"

    headers = {
        "X-Tableau-Auth": token,
        "Accept": "application/json"
    }

    response = requests.get(url, headers=headers)
    response.raise_for_status()

    return response.json().get("views", {}).get("view", [])
