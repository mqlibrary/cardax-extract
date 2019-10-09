import json
import requests
import databank_model
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import relationship, sessionmaker
from cardaxdb_model import Cardholder, Card, AccessGroup, BaseCardax


class CardaxDAO:
    def __init__(self, apikey):
        self.session = requests.Session()
        self.session.headers = {"Authorization": apikey}

    def fetch_cardholder(self, id):
        r = self.session.get("https://10.15.225.71:8904/api/cardholders/" + id)
        return r.json()

    def fetch_cardholders(self, skip=0, top=1000):
        r = self.session.get("https://10.15.225.71:8904/api/cardholders",
                             params={"sort": "id", "skip": skip, "top": top})
        return r.json().get("results") if "results" in r.json() else []

    def make_access_groups(self, access_group):
        return AccessGroup(id=access_group["id"], name=access_group["name"])

    def fetch_access_groups(self, skip=0, top=10000):
        r = self.session.get("https://10.15.225.71:8904/api/access_groups",
                             params={"sort": "id", "skip": skip, "top": top})
        return r.json().get("results") if "results" in r.json() else []


# if __name__ == "__main__":
#     log.info("fetching party ids")
#     party_ids = databank_model.get_party_ids()

#     engine = create_engine(config.cardaxdb_conn)
#     Base.metadata.drop_all(engine)
#     Base.metadata.create_all(engine)
#     Base.metadata.bind = engine
#     Session = sessionmaker(bind=engine, expire_on_commit=False)

#     pool = MP.Pool(processes=NUM_PROCESSES)

#     session = Session()

#     log.info("adding access groups")
#     access_groups = fetch_access_groups()
#     results = pool.imap(make_access_groups, access_groups)
#     session.add_all(results)
#     session.commit()

#     access_group_list = {}
#     for access_group in access_groups:
#         access_group_list[access_group["id"]] = session.query(AccessGroup).get(access_group["id"])

#     BATCH_SIZE = 2000
#     for x in range(90):
#         offset = x * BATCH_SIZE
#         log.info("adding cardholders {} to {}".format(offset, offset + BATCH_SIZE))
#         cardholders = fetch_cardholders(offset, BATCH_SIZE)
#         log.info("fetched cardholder: {}".format(len(cardholders)))

#         args = []
#         for cardholder in cardholders:
#             args.append((party_ids, access_group_list, cardholder))

#         results = pool.starmap(make_cardholder, args)
#         for result in results:
#             session.merge(result)

#         session.commit()

#         if len(cardholders) < BATCH_SIZE:
#             break

#     log.info("done.")
