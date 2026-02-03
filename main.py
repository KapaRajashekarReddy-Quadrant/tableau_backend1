import uuid
import os
import requests
from flask import Flask, request
from flask_restx import Api, Resource, fields
from flask_cors import CORS
from azure.storage.blob import BlobServiceClient, ContentSettings

# ================== CONFIG ==================
TABLEAU_SERVER = "https://prod-in-a.online.tableau.com"
API_VERSION = "3.27"
DOWNLOAD_DIR = "downloads"
TIMEOUT = 30

# -------- AZURE (BASELINE CONFIG) --------
AZURE_CONNECTION_STRING = (
    "DefaultEndpointsProtocol=https;AccountName=tablueatopowerbi;AccountKey=eFWeORWAr+WppTlE4iKjKostmcQybHoajyjdduHVu10rXza/o1S0AfP+Im6vnG/kDC1UOcRiJyoj+AStN92bog==;EndpointSuffix=core.windows.net"
)
AZURE_CONTAINER_NAME = "tabluea-raw"

os.makedirs(DOWNLOAD_DIR, exist_ok=True)
TOKEN_STORE = {}

# ================== APP INIT ==================
app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}}, supports_credentials=True)

api = Api(
    app,
    title="Tableau REST API + Azure Blob",
    version="1.0",
    description="Signin, Metadata, Connections, TWBX/TDSX download to Azure"
)

ns = api.namespace("tableau", description="Tableau Operations")

# ================== MODELS ==================
signin_model = api.model("Signin", {
    "username": fields.String(required=True),
    "password": fields.String(required=True),
    "site_content_url": fields.String(default="")
})

token_model = api.model("Token", {
    "api_token": fields.String(required=True)
})

workbook_model = api.model("Workbook", {
    "api_token": fields.String(required=True),
    "workbook_id": fields.String(required=True)
})

download_workbook_model = api.model("DownloadWorkbook", {
    "api_token": fields.String(required=True),
    "workbook_id": fields.String(required=True),
    "file_name": fields.String(required=False)
})

download_ds_model = api.model("DownloadWorkbookDatasources", {
    "api_token": fields.String(required=True),
    "workbook_id": fields.String(required=True)
})

# ================== HELPERS ==================
def safe_request(method, url, headers=None, json_body=None, stream=False):
    r = requests.request(
        method,
        url,
        headers=headers,
        json=json_body,
        stream=stream,
        timeout=TIMEOUT
    )
    if r.status_code not in (200, 201):
        raise RuntimeError(f"{r.status_code} - {r.text}")
    return r


def get_auth(api_token):
    if api_token not in TOKEN_STORE:
        raise RuntimeError("Invalid or expired api_token")
    return TOKEN_STORE[api_token]


def upload_to_azure(file_path, blob_name):
    blob_service = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    blob_client = blob_service.get_blob_client(
        container=AZURE_CONTAINER_NAME,
        blob=blob_name
    )
    with open(file_path, "rb") as f:
        blob_client.upload_blob(
            f,
            overwrite=True,
            content_settings=ContentSettings(content_type="application/octet-stream")
        )
    return blob_client.url

# ================== SIGN IN ==================
@ns.route("/signin")
class SignIn(Resource):
    @ns.expect(signin_model)
    def post(self):
        try:
            body = request.json
            payload = {
                "credentials": {
                    "name": body["username"],
                    "password": body["password"],
                    "site": {"contentUrl": body.get("site_content_url", "")}
                }
            }

            r = safe_request(
                "POST",
                f"{TABLEAU_SERVER}/api/{API_VERSION}/auth/signin",
                headers={"Content-Type": "application/json", "Accept": "application/json"},
                json_body=payload
            )

            creds = r.json()["credentials"]
            api_token = str(uuid.uuid4())

            TOKEN_STORE[api_token] = {
                "auth_token": creds["token"],
                "site_id": creds["site"]["id"]
            }

            return {"api_token": api_token}, 200

        except Exception as e:
            return {"error": "Signin failed", "details": str(e)}, 401

# ================== FETCH METADATA (FLAT JSON) ==================
@ns.route("/fetch_data")
class FetchData(Resource):
    @ns.expect(token_model)
    def post(self):
        try:
            auth = get_auth(request.json["api_token"])
            headers = {"X-Tableau-Auth": auth["auth_token"], "Accept": "application/json"}
            base = f"{TABLEAU_SERVER}/api/{API_VERSION}/sites/{auth['site_id']}"

            projects = safe_request("GET", f"{base}/projects", headers).json()["projects"]["project"]
            workbooks = safe_request("GET", f"{base}/workbooks", headers).json()["workbooks"]["workbook"]
            views = safe_request("GET", f"{base}/views", headers).json()["views"]["view"]
            datasources = safe_request("GET", f"{base}/datasources", headers).json()["datasources"]["datasource"]

            return {
                "projects": [
                    {"id": p["id"], "name": p["name"], "parent_id": p.get("parentProjectId")}
                    for p in projects
                ],
                "workbooks": [
                    {"id": w["id"], "name": w["name"], "project_id": w.get("project", {}).get("id")}
                    for w in workbooks
                ],
                "views": [
                    {"id": v["id"], "name": v["name"], "workbook_id": v.get("workbook", {}).get("id")}
                    for v in views
                ],
                "datasources": [
                    {"id": d["id"], "name": d["name"], "project_id": d.get("project", {}).get("id")}
                    for d in datasources
                ]
            }, 200

        except Exception as e:
            return {"error": str(e)}, 400

# ================== WORKBOOK → DATASOURCES ==================
@ns.route("/workbook_datasources")
class WorkbookDatasources(Resource):
    @ns.expect(workbook_model)
    def post(self):
        try:
            body = request.json
            auth = get_auth(body["api_token"])
            headers = {"X-Tableau-Auth": auth["auth_token"], "Accept": "application/json"}

            url = f"{TABLEAU_SERVER}/api/{API_VERSION}/sites/{auth['site_id']}/workbooks/{body['workbook_id']}/connections"
            connections = safe_request("GET", url, headers).json()["connections"]["connection"]

            datasources = []
            for c in connections:
                if c.get("datasource"):
                    datasources.append({
                        "datasource_name": c["datasource"]["name"],
                        "datasource_id": c["datasource"]["id"],
                        "published": True
                    })

            return {
                "workbook_id": body["workbook_id"],
                "datasources": datasources
            }, 200

        except Exception as e:
            return {"error": str(e)}, 400

# ================== TECHNICAL CONNECTION DETAILS ==================
@ns.route("/get_connections")
class GetConnections(Resource):
    @ns.expect(workbook_model)
    def post(self):
        try:
            body = request.json
            auth = get_auth(body["api_token"])
            headers = {"X-Tableau-Auth": auth["auth_token"], "Accept": "application/json"}

            url = f"{TABLEAU_SERVER}/api/{API_VERSION}/sites/{auth['site_id']}/workbooks/{body['workbook_id']}/connections"
            connections = safe_request("GET", url, headers).json()["connections"]["connection"]

            return {
                "workbook_id": body["workbook_id"],
                "connections": connections
            }, 200

        except Exception as e:
            return {"error": str(e)}, 400

# ================== DOWNLOAD WORKBOOK (.twbx) ==================
@ns.route("/download_workbook")
class DownloadWorkbook(Resource):
    @ns.expect(download_workbook_model)
    def post(self):
        try:
            body = request.json
            auth = get_auth(body["api_token"])

            filename = body.get("file_name", f"{body['workbook_id']}.twbx")
            local_path = os.path.join(DOWNLOAD_DIR, filename)

            url = f"{TABLEAU_SERVER}/api/{API_VERSION}/sites/{auth['site_id']}/workbooks/{body['workbook_id']}/content"
            r = safe_request("GET", url, {"X-Tableau-Auth": auth["auth_token"]}, stream=True)

            with open(local_path, "wb") as f:
                for chunk in r.iter_content(8192):
                    f.write(chunk)

            blob_url = upload_to_azure(local_path, filename)
            os.remove(local_path)

            return {"blob_url": blob_url}, 200

        except Exception as e:
            return {"error": str(e)}, 400



# ================== DOWNLOAD WORKBOOK DATASOURCES (.tdsx) ==================
@ns.route("/download_workbook_datasources")
class DownloadWorkbookDatasources(Resource):
    @ns.expect(download_ds_model)
    def post(self):
        try:
            body = request.json
            auth = get_auth(body["api_token"])
            headers = {"X-Tableau-Auth": auth["auth_token"], "Accept": "application/json"}
            base = f"{TABLEAU_SERVER}/api/{API_VERSION}/sites/{auth['site_id']}"

            published = safe_request("GET", f"{base}/datasources", headers).json()
            published_map = {
                ds["id"]: ds["name"]
                for ds in published["datasources"]["datasource"]
            }

            connections = safe_request(
                "GET",
                f"{base}/workbooks/{body['workbook_id']}/connections",
                headers
            ).json()["connections"]["connection"]

            uploaded, skipped = [], []

            for c in connections:
                ds = c.get("datasource")
                if not ds or ds["id"] not in published_map:
                    skipped.append({
                        "datasource_name": ds["name"] if ds else None,
                        "reason": "Embedded datasource"
                    })
                    continue

                name = published_map[ds["id"]].replace(" ", "_") + ".tdsx"
                local_path = os.path.join(DOWNLOAD_DIR, name)

                r = safe_request(
                    "GET",
                    f"{base}/datasources/{ds['id']}/content",
                    {"X-Tableau-Auth": auth["auth_token"]},
                    stream=True
                )

                with open(local_path, "wb") as f:
                    for chunk in r.iter_content(8192):
                        f.write(chunk)

                blob_url = upload_to_azure(local_path, name)
                os.remove(local_path)

                uploaded.append({
                    "datasource_name": published_map[ds["id"]],
                    "blob_url": blob_url
                })

            return {
                "uploaded": uploaded,
                "skipped": skipped
            }, 200

        except Exception as e:
            return {"error": "Datasource download failed", "details": str(e)}, 400

# ================== RUN ==================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)


