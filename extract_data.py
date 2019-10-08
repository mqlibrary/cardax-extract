import config
import logging as log
import json
import requests
import multiprocessing as MP
from cardax_dao import CardaxDAO
import cardaxdb_model
from cardaxdb_dao import CardaxDbDAO
import databank_model
from databank_dao import DatabankDAO
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import relationship, sessionmaker
from cardax_model import Cardholder, Card, AccessGroup, BaseCardax

log.basicConfig(level=log.INFO, format="[%(asctime)s][%(levelname)s]: %(message)s")

NUM_PROCESSES = 10

if __name__ == "__main__":
    log.info("extracting data from cardax and databank")

    log.info("initiailsing engines")
    pool = MP.Pool(processes=NUM_PROCESSES)
    engine_cardax = create_engine(config.cardaxdb_conn)
    engine_databank = create_engine(config.databank_conn)
    databank_dao = DatabankDAO(engine_databank)
    cardaxdb_dao = CardaxDbDAO(engine_cardax)
    cardax_dao = CardaxDAO(config.cardax_apikey)

    log.info("fetching databank party ids")
    party_ids = databank_dao.get_party_ids()
    log.info("found %s: %s", "partyIds", len(party_ids))

    log.info("fetching access groups from cardax")
    access_groups = cardax_dao.fetch_access_groups()
    results = pool.imap(cardax_dao.make_access_groups, access_groups)
    log.info("found %s: %s", "accessGroups", len(party_ids))

    log.info("saving access groups to cardaxdb")
    cardaxdb_dao.save_access_groups(results)

    log.info("creating access group map")
    access_group_list = {}
    for access_group in access_groups:
        access_group_list[access_group["id"]] = cardaxdb_dao.get_access_group(access_group["id"])
    log.info("mapped access groups: %s", len(access_group_list))

    BATCH_SIZE = 2000
    for x in range(90):
        offset = x * BATCH_SIZE
        log.info("adding cardholders {} to {}".format(offset, offset + BATCH_SIZE))
        cardholders = cardax_dao.fetch_cardholders(offset, BATCH_SIZE)
        log.info("fetched cardholder: {}".format(len(cardholders)))

        args = []
        for cardholder in cardholders:
            args.append((party_ids, access_group_list, cardholder))

        results = pool.starmap(cardax_dao.make_cardholder, args)

        if len(cardholders) < BATCH_SIZE:
            break

    log.info("extraction complete")
