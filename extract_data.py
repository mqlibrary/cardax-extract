import config
import sys
import json
import requests
import logging as log
import multiprocessing as MP
from cardax_dao import CardaxDAO
import cardaxdb_model
from cardaxdb_dao import CardaxDbDAO
import databank_model
from databank_dao import DatabankDAO
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import relationship, sessionmaker
from cardaxdb_model import Cardholder, Card, AccessGroup, BaseCardax

log.basicConfig(level=log.INFO, format="[%(asctime)s][%(levelname)s]: %(message)s")

NUM_PROCESSES = 10


def extract_cardax_data():
    log.info("extracting data from cardax")

    log.info("initialising engines")
    pool = MP.Pool(processes=NUM_PROCESSES)
    cardax_dao = CardaxDAO(config.cardax_apikey)
    databank_dao = DatabankDAO(create_engine(config.databank_conn))
    cardaxdb_dao = CardaxDbDAO(create_engine(config.cardaxdb_conn))
    cardaxdb_dao.initialise_schema_cardax()

    log.info("fetching databank party ids...")
    party_ids = databank_dao.get_party_ids()
    log.info("found party ids: %s", len(party_ids))

    log.info("fetching access groups from cardax...")
    access_groups = cardax_dao.fetch_access_groups()
    results = pool.imap(cardax_dao.make_access_groups, access_groups)
    log.info("found access groups: %s", len(access_groups))

    log.info("saving access groups to cardaxdb...")
    cardaxdb_dao.save(results)
    log.info("saved")

    log.info("creating access group map...")
    access_group_list = {}
    for access_group in access_groups:
        access_group_list[access_group["id"]] = cardaxdb_dao.get_access_group(access_group["id"])
    log.info("mapped access groups: %s", len(access_group_list))

    BATCH_SIZE = 5000
    for x in range(36):
        offset = x * BATCH_SIZE
        log.info("fetching cardholders {} to {}".format(offset, offset + BATCH_SIZE))
        cardholders = cardax_dao.fetch_cardholders(offset, BATCH_SIZE)

        log.info("fetching cardholder details".format(offset, offset + BATCH_SIZE))
        args = []
        for cardholder in cardholders:
            args.append(cardholder["id"])
        cxCardholders = pool.map(cardax_dao.fetch_cardholder, args)

        log.info("constructing cardholders")
        results = []
        for cxCardholder in cxCardholders:
            results.append(cardaxdb_dao.make_cardholder(party_ids, access_group_list, cxCardholder))

        log.info("saving cardholders")
        cardaxdb_dao.update(results)

        if len(cardholders) < BATCH_SIZE:
            break

    log.info("extraction complete")


def extract_databank_data():
    log.info("extracting data from databank")

    log.info("initialising engines")
    databank_dao = DatabankDAO(create_engine(config.databank_conn))
    cardaxdb_dao = CardaxDbDAO(create_engine(config.cardaxdb_conn))
    cardaxdb_dao.initialise_schema_databank()

    log.info("fetching unicard cards")
    unicard_cards = databank_dao.get_unicard_cards()

    log.info("saving unicard cards: %s", len(unicard_cards))
    cardaxdb_dao.save(unicard_cards)

    log.info("fetching databank patrons")
    databank_patrons = databank_dao.get_databank_patrons()

    log.info("saving databank patrons: %s", len(databank_patrons))
    cardaxdb_dao.save(databank_patrons)

    log.info("extraction complete")


def print_help():
    print("{} ({}|{})".format(sys.argv[0], 'databank', 'cardax'))


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == 'databank':
        log.info("extract databank data")
        extract_databank_data()
    elif len(sys.argv) > 1 and sys.argv[1] == 'cardax':
        log.info("extract cardax data")
        extract_cardax_data()
    else:
        print_help()
        sys.exit()
