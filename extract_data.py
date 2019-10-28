import config
import sys
import json
import requests
import logging as log
import multiprocessing as MP
from elastic_dao import ElasticDAO
from cardax_dao import CardaxDAO
import cardaxdb_model
from cardaxdb_dao import CardaxDbDAO
from databank_dao import DatabankDAO
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import relationship, sessionmaker
from cardaxdb_model import Cardholder, AccessGroup, AccessZone, Door

log.basicConfig(
    level=log.INFO, format="[%(asctime)s][%(levelname)s]: %(message)s")


def extract_cardax_data():
    log.info("extracting data from cardax")

    log.info("initialising engines")
    cardax_dao = CardaxDAO(config.cardax_apikey, config.cardax_baseurl)
    databank_dao = DatabankDAO(create_engine(config.databank_conn))
    cardaxdb_dao = CardaxDbDAO(create_engine(config.cardaxdb_conn))
    cardaxdb_dao.initialise_schema_cardax()

    log.info("updating cardax doors")
    doors = cardax_dao.fetch_doors()
    entities = [Door(id=c["id"], name=c["name"]) for c in doors]
    if len(entities) > 0:
        cardaxdb_dao.update(entities, type(entities[0]))
    log.info("updated doors: %s", len(entities))

    log.info("updating cardax access zones")
    access_zones = cardax_dao.fetch_access_zones()
    entities = [AccessZone(id=c["id"], name=c["name"]) for c in access_zones]
    if len(entities) > 0:
        cardaxdb_dao.update(entities, type(entities[0]))
    log.info("updated access zones: %s", len(entities))

    log.info("updating cardax event groups")
    event_groups = cardax_dao.fetch_event_groups()
    entities = [cardaxdb_dao.make_event_group(eg) for eg in event_groups]
    if len(entities) > 0:
        cardaxdb_dao.update(entities, type(entities[0]))
    log.info("updated cardax access groups: %s", len(entities))

    log.info("updating cardax access groups")
    access_groups = cardax_dao.fetch_access_groups()
    entities = [AccessGroup(id=ag["id"], name=ag["name"]) for ag in access_groups]
    if len(entities) > 0:
        cardaxdb_dao.update(entities, type(entities[0]))
    log.info("updated cardax access groups: %s", len(entities))

    log.info("creating access group map...")
    access_group_list = {}
    for access_group in access_groups:
        access_group_list[access_group["id"]
                          ] = cardaxdb_dao.get_access_group(access_group["id"])
    log.info("mapped access groups: %s", len(access_group_list))

    log.info("fetching databank party ids...")
    party_ids = databank_dao.get_party_ids()
    log.info("found party ids: %s", len(party_ids))

    BATCH_SIZE = 5000
    for x in range(36):
        offset = x * BATCH_SIZE
        log.info("fetching cardholders {} to {}".format(
            offset, offset + BATCH_SIZE))
        cardholders = cardax_dao.fetch_cardholders(offset, BATCH_SIZE)

        log.info("fetching cardholder details")
        pool = MP.Pool(processes=10)
        args = [c["id"] for c in cardholders]
        cxCardholders = pool.map(cardax_dao.fetch_cardholder, args)

        log.info("constructing cardholders")
        entities = [cardaxdb_dao.make_cardholder(
            party_ids, access_group_list, c) for c in cxCardholders]

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


def extract_events(pos=None):
    log.info("extracting data from cardax events")

    log.info("initialising engines")
    elastic_dao = ElasticDAO(config.elastic_url, config.elastic_usr, config.elastic_pwd, "cardax-events", "event")
    cardax_dao = CardaxDAO(config.cardax_apikey, config.cardax_baseurl)
    cardaxdb_dao = CardaxDbDAO(create_engine(config.cardaxdb_conn))
    cardaxdb_dao.initialise_schema_cardax()

    max_pos = cardaxdb_dao.get_max_pos() if pos is None else pos

    BATCH_SIZE = 5000
    log.info("fetching events from: %s", max_pos)
    events, pos = cardax_dao.fetch_events(group=23, doors=",".join(config.cardax_doors), pos=max_pos, top=BATCH_SIZE)
    while len(events) > 0:
        log.info("saving events[%s]: %s", pos, len(events))
        entities = [cardaxdb_dao.make_event(e) for e in events]
        if len(entities) > 0:
            cardaxdb_dao.update(entities, type(entities[0]))
        log.info("fetching events from: %s", pos)
        events, pos = cardax_dao.fetch_events(group=23, doors=",".join(config.cardax_doors), pos=pos, top=BATCH_SIZE)

    log.info("extraction complete")


def elasticsearch_load(pos=None):
    log.info("extracting data from databank")

    log.info("initialising engines")
    cardaxdb_dao = CardaxDbDAO(create_engine(config.cardaxdb_conn))
    elastic_dao = ElasticDAO(config.elastic_url, config.elastic_usr, config.elastic_pwd, "cardax-events", "event")

    max_pos = elastic_dao.get_max_pos() if pos is None else pos
    log.info("fetching events from databank: %s", max_pos)
    events = cardaxdb_dao.get_events(max_pos)
    log.info("found events: %s", len(events))
    BATCH_SIZE = 5000
    event_batch = []
    for idx, event in enumerate(events):
        event_batch.append(event)
        if len(event_batch) >= BATCH_SIZE:
            log.info("saving events [%s/%s]", idx, len(events))
            elastic_dao.save_events(event_batch)
            event_batch = []

    if len(event_batch) > 0:
        log.info("saving events [%s/%s]", idx, len(events))
        elastic_dao.save_events(event_batch)

    log.info("extraction complete")


def print_help():
    print("{} ({} | {} | {})".format(
        sys.argv[0], 'databank', 'cardax', 'events', 'esload'))


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == 'esload':
        elasticsearch_load()
    elif len(sys.argv) > 1 and sys.argv[1] == 'databank':
        extract_databank_data()
    elif len(sys.argv) > 1 and sys.argv[1] == 'cardax':
        extract_cardax_data()
    elif len(sys.argv) > 1 and sys.argv[1] == 'events':
        pos = None
        if len(sys.argv) > 2 and sys.argv[2]:
            pos = int(sys.argv[2])
        extract_events(pos)
    else:
        print_help()
        sys.exit()
