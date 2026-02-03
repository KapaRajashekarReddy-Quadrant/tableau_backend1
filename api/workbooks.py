import requests
from config import TABLEAU_SERVER, API_VERSION

def get_all_workbooks(token, site_id):
    url = f"{TABLEAU_SERVER}/api/{API_VERSION}/sites/{site_id}/workbooks"

    headers = {
        "X-Tableau-Auth": token,
        "Accept": "application/json"
    }

    response = requests.get(url, headers=headers)
    response.raise_for_status()

    return response.json().get("workbooks", {}).get("workbook", [])


def download_workbook(token, site_id, workbook_id, file_path):
    url = f"{TABLEAU_SERVER}/api/{API_VERSION}/sites/{site_id}/workbooks/{workbook_id}/content"

    headers = {
        "X-Tableau-Auth": token
    }

    response = requests.get(url, headers=headers, stream=True)
    response.raise_for_status()

    with open(file_path, "wb") as f:
        for chunk in response.iter_content(8192):
            f.write(chunk)

    return file_path
