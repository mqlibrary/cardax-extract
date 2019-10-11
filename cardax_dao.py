import json
import requests
import databank_model
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import relationship, sessionmaker
from cardaxdb_model import BaseCardax, Cardholder, Card, AccessGroup


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

    # item type 11 = Door
    def fetch_doors(self, skip=0, top=10000):
        r = self.session.get(self.baseurl + "/items",
                             params={"type": "11", "name": "C3C", "skip": skip, "top": top})
        return r.json().get("results") if "results" in r.json() else []

    # item type 12 = Access Zone
    def fetch_access_zones(self, skip=0, top=10000):
        r = self.session.get(self.baseurl + "/items",
                             params={"type": "12", "name": "C3C", "skip": skip, "top": top})
        return r.json().get("results") if "results" in r.json() else []
