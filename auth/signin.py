import requests
from config import TABLEAU_SERVER, API_VERSION

def signin_with_credentials(username, password, site_content_url=""):
    url = f"{TABLEAU_SERVER}/api/{API_VERSION}/auth/signin"

    payload = {
        "credentials": {
            "name": username,
            "password": password,
            "site": {"contentUrl": site_content_url}
        }
    }

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()

    data = response.json()
    return data["credentials"]["token"], data["credentials"]["site"]["id"]
