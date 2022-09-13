import requests
from cachetools import cached


@cached(cache={})
def get_token(BASE, USER, PASS):
    print("Requesting token...")
    return requests.post(
        f"{BASE}/api/session/",
        json={"username": USER, "password": PASS},
    ).json()["id"]
