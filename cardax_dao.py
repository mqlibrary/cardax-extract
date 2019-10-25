import re
import json
import requests


class CardaxDAO:
    def __init__(self, apikey, baseurl):
        self.session = requests.Session()
        self.session.headers = {"Authorization": apikey}
        self.baseurl = baseurl

    def fetch_cardholder(self, id):
        r = self.session.get(self.baseurl + "/cardholders/" + id)
        return r.json()

    def fetch_cardholders(self, skip=0, top=1000):
        r = self.session.get(self.baseurl + "/cardholders",
                             params={"sort": "id", "skip": skip, "top": top})
        return r.json().get("results") if "results" in r.json() else []

    def fetch_access_groups(self, skip=0, top=10000):
        r = self.session.get(self.baseurl + "/access_groups",
                             params={"sort": "id", "skip": skip, "top": top})
        return r.json().get("results") if "results" in r.json() else []

    def fetch_events(self, group, doors, pos=None, before=None, after=None, top=10):
        params = {}
        params["previous"] = False
        params["group"] = group
        params["source"] = doors
        params["top"] = top
        if pos is not None:
            params["pos"] = pos

        if before is not None:
            params["before"] = before

        if after is not None:
            params["after"] = after

        r = self.session.get(self.baseurl + "/events", params=params)

        if r.json().get("next").get("href"):
            m = re.search(r"pos=(\d+)", r.json().get("next").get("href"))
            next_pos = m.group(1) if m is not None else None

        return (r.json().get("events"), next_pos) if "events" in r.json() else ([], next_pos)

    def fetch_items(self, type, skip=0, top=10000):
        r = self.session.get(self.baseurl + "/items",
                             params={"type": type, "skip": skip, "top": top})
        return r.json().get("results") if "results" in r.json() else []

    # item type 11 = Door
    def fetch_doors(self, skip=0, top=10000):
        return self.fetch_items("11", skip, top)

    # item type 12 = Access Zone
    def fetch_access_zones(self, skip=0, top=10000):
        return self.fetch_items("12", skip, top)
