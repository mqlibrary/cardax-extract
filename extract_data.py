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
from cardaxdb_model import BaseCardax, Cardholder, Card, AccessGroup

log.basicConfig(level=log.INFO, format="[%(asctime)s][%(levelname)s]: %(message)s")


def extract_cardax_data():
    log.info("extracting data from cardax")

    log.info("initialising engines")
    cardax_dao = CardaxDAO(config.cardax_apikey, config.cardax_baseurl)
    databank_dao = DatabankDAO(create_engine(config.databank_conn))
    cardaxdb_dao = CardaxDbDAO(create_engine(config.cardaxdb_conn))
    cardaxdb_dao.initialise_schema_cardax()

    log.info("fetching access groups from cardax...")
    access_groups = cardax_dao.fetch_access_groups()
    entities = [AccessGroup(id=ag["id"], name=ag["name"]) for ag in access_groups]

    log.info("saving access groups to cardaxdb...")
    if len(entities) > 0:
        cardaxdb_dao.update(entities, type(entities[0]))
    log.info("saved access groups: %s", len(entities))

    log.info("creating access group map...")
    access_group_list = {}
    for access_group in access_groups:
        access_group_list[access_group["id"]] = cardaxdb_dao.get_access_group(access_group["id"])
    log.info("mapped access groups: %s", len(access_group_list))

    log.info("fetching databank party ids...")
    party_ids = databank_dao.get_party_ids()
    log.info("found party ids: %s", len(party_ids))

    BATCH_SIZE = 5000
    for x in range(36):
        offset = x * BATCH_SIZE
        log.info("fetching cardholders {} to {}".format(offset, offset + BATCH_SIZE))
        cardholders = cardax_dao.fetch_cardholders(offset, BATCH_SIZE)

        log.info("fetching cardholder details".format(offset, offset + BATCH_SIZE))
        pool = MP.Pool(processes=10)
        args = [c["id"] for c in cardholders]
        cxCardholders = pool.map(cardax_dao.fetch_cardholder, args)

        log.info("constructing cardholders")
        entities = [cardaxdb_dao.make_cardholder(party_ids, access_group_list, c) for c in cxCardholders]

        log.info("saving cardholders")
        if len(entities) > 0:
            cardaxdb_dao.update(entities, type(entities[0]))
        log.info("saved cardholders: %s", len(entities))

        if len(cxCardholders) < BATCH_SIZE:
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
    if len(unicard_cards) > 1:
        cardaxdb_dao.update(unicard_cards, type(unicard_cards[0]))

    log.info("fetching databank patrons")
    databank_patrons = databank_dao.get_databank_patrons()

    log.info("saving databank patrons: %s", len(databank_patrons))
    if len(databank_patrons) > 1:
        cardaxdb_dao.update(databank_patrons, type(databank_patrons[0]))

    log.info("extraction complete")


def extract_event_data():
    log.info("extracting data from databank")

    log.info("initialising engines")
    cardax_dao = CardaxDAO(config.cardax_apikey, config.cardax_baseurl)
    cardaxdb_dao = CardaxDbDAO(create_engine(config.cardaxdb_conn))
    cardaxdb_dao.initialise_schema_events()

    log.info("updating cardax doors")
    cxDoors = cardax_dao.fetch_doors()

    entities = [cardaxdb_dao.make_door(cxDoor) for cxDoor in cxDoors]
    if len(entities) > 0:
        cardaxdb_dao.update(entities, type(entities[0]))
    log.info("updated doors: %s", len(entities))

    log.info("updating cardax access zones")
    cxAccessZones = cardax_dao.fetch_access_zones()

    entities = [cardaxdb_dao.make_access_zone(cxAccessZone) for cxAccessZone in cxAccessZones]
    if len(entities) > 0:
        entity_type = type(entities[0])
        cardaxdb_dao.update(entities, entity_type)
    log.info("updated access zones: %s", len(entities))

    log.info("extraction complete")


def print_help():
    print("{} ({} | {} | {})".format(sys.argv[0], 'databank', 'cardax', 'events'))


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == 'databank':
        extract_databank_data()
    elif len(sys.argv) > 1 and sys.argv[1] == 'cardax':
        extract_cardax_data()
    elif len(sys.argv) > 1 and sys.argv[1] == 'events':
        extract_event_data()
    else:
        print_help()
        sys.exit()
