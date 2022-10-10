import json
import requests
from common import get_token


BASE = "<>"
PASS = "<>"
USER = "<>"
COLLECTION = "<>"


def headers():
    return {"X-Metabase-Session": get_token(BASE, USER, PASS)}


def get_collection(old):
    collections = requests.get(f"{BASE}/api/collection/", headers=headers()).json()
    collections = [c for c in collections if c["name"] == old["name"]]
    if collections:
        return collections[0]["id"]
    else:
        return requests.post(
            f"{BASE}/api/collection/", headers=headers(), json=old
        ).json()["id"]


def delete_model(id, model):
    return requests.delete(
        f"http://localhost:3000/api/{model}/{id}",
        headers=headers(),
    ).json()


def clear_collection(id):
    items = requests.get(
        f"http://localhost:3000/api/collection/{id}/items",
        headers=headers(),
    ).json()

    for item in items["data"]:
        try:
            delete_model(item["id"], item["model"])
        except:
            pass


def update_collection(id, c):
    clear_collection(id)
    out = requests.put(
        f"http://localhost:3000/api/collection/{id}",
        headers=headers(),
        json=c,
    ).json()
    return out["id"]


def create_card(c):
    out = requests.post(
        f"http://localhost:3000/api/card/", headers=headers(), json=c
    ).json()
    return out["id"]


def create_dashboard(c, cache):
    id = requests.post(
        f"http://localhost:3000/api/dashboard/", headers=headers(), json=c
    ).json()["id"]
    for card in c["ordered_cards"]:
        card["cardId"] = cache["card__" + str(card["card_id"])]
        out = requests.post(
            f"http://localhost:3000/api/dashboard/{id}/cards",
            headers=headers(),
            json=card,
        ).text
        print(out)
    return out


def get_databases():
    items = requests.get(
        f"{BASE}/api/database?include=tables", headers=headers()
    ).json()
    out = {}
    for db in items["data"]:
        out[db["name"]] = {
            "tables": dict((t["name"], t["id"]) for t in db["tables"]),
            "id": db["id"],
        }
    return out


def map_table(src_dbs, dst_dbs, db, table):
    src_db = src_dbs[str(db)]
    dst_db = dst_dbs[src_db["name"]]

    if table is not None:
        src_table = src_db["tables"][str(table)]
        dst_table = dst_db["tables"][src_table]
    else:
        dst_table = None

    return dst_db["id"], dst_table


def process_and_upload_card(c, src_dbs, dst_dbs, cards, cache={}):
    if f"card__{c['id']}" in cache:
        return

    def find_card(id):
        return [c for c in cards if f"card__{c['id']}" == id][0]

    def table_mapper(table):
        if isinstance(table, str):
            process_and_upload_card(find_card(table), src_dbs, dst_dbs, cards, cache)
            return "card__" + str(cache[table])
        else:
            return map_table(src_dbs, dst_dbs, src_db, table)[1]

    c["collection_id"] = id
    src_db = c["database_id"]
    c["database_id"] = map_table(src_dbs, dst_dbs, src_db, c["table_id"])[0]
    dataset_query = c["dataset_query"]
    dataset_query["database"] = c["database_id"]
    if "query" in dataset_query:
        query = dataset_query["query"]
        if "source-table" in query:
            query["source-table"] = table_mapper(query["source-table"])
        if "source-query" in query and "source-table" in query["source-query"]:
            query["source-query"]["source-table"] = table_mapper(
                query["source-query"]["source-table"]
            )
        for join in query.get("joins", []):
            join["source-table"] = table_mapper(join["source-table"])
    cache[f"card__{c['id']}"] = create_card(c)


if __name__ == "__main__":
    dbs = get_databases()
    data = json.load(open(f"{COLLECTION}.json"))
    src_dbs = data["db"]
    id = get_collection(data["collection"])
    update_collection(id, data["collection"])
    cards, dashboards = data["cards"], data["dashboards"]

    cache = {}
    for c in cards:
        process_and_upload_card(c, src_dbs, dbs, cards, cache)

    for c in dashboards:
        c["collection_id"] = id
        create_dashboard(c, cache)
