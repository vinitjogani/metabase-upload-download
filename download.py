import json
import requests
from common import get_token

BASE = "<>"
PASS = "<>"
USER = "<>"
COLLECTION = "<>"


def headers():
    return {"X-Metabase-Session": get_token(BASE, USER, PASS)}


def get_collections():
    collections = requests.get(f"{BASE}/api/collection/", headers=headers()).json()
    return dict((c["name"], c) for c in collections if not c.get("archived"))


def get_card(id):
    card = requests.get(f"{BASE}/api/card/{id}", headers=headers()).json()
    return card


def get_dashboard(id):
    dash = requests.get(f"{BASE}/api/dashboard/{id}", headers=headers()).json()
    return dash


def get_collection(id):
    items = requests.get(f"{BASE}/api/collection/{id}/items", headers=headers()).json()
    cards = [get_card(c["id"]) for c in items["data"] if c["model"] == "card"]
    dashboards = [
        get_dashboard(c["id"]) for c in items["data"] if c["model"] == "dashboard"
    ]
    return cards, dashboards


def get_databases():
    items = requests.get(
        f"{BASE}/api/database?include=tables", headers=headers()
    ).json()
    out = {}
    for db in items["data"]:
        out[db["id"]] = {
            "name": db["name"],
            "tables": dict((t["id"], t["name"]) for t in db["tables"]),
        }
    return out


if __name__ == "__main__":
    dbs = get_databases()
    collections = get_collections()
    cards, dashboards = get_collection(collections[COLLECTION]["id"])
    output = {
        "collection": collections[COLLECTION],
        "cards": cards,
        "dashboards": dashboards,
        "db": dbs,
    }

    json.dump(output, open(f"{COLLECTION}.json", "w"))
