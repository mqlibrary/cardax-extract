import sqlalchemy as db
from datetime import datetime
from sqlalchemy import func
from sqlalchemy.orm import sessionmaker, joinedload
from cardaxdb_model import BaseCardax, Cardholder, AccessGroup, Card, BaseDatabank, Patron, CardOneID, UnicardCard, BaseEvents, AccessZone, Door, Event, EventGroup, EventType


class CardaxDbDAO:
    def __init__(self, engine):
        self.engine = engine
        self.Session = sessionmaker(bind=self.engine, expire_on_commit=False)
        self.access_group_session = self.Session()
        self.cardholder_session = self.Session()
        self.cardholder_query = self.cardholder_session.query(Cardholder)

    def initialise_schema_databank(self):
        BaseDatabank.metadata.drop_all(self.engine)
        BaseDatabank.metadata.create_all(self.engine)
        BaseDatabank.metadata.bind = self.engine

    def initialise_schema_cardax(self):
        BaseCardax.metadata.create_all(self.engine)
        BaseCardax.metadata.bind = self.engine

    def get_access_group(self, id):
        return self.access_group_session.query(AccessGroup).get(id)

    def get_max_pos(self):
        session = self.Session()
        return session.query(func.max(Event.id)).scalar()

    def make_cardholder(self, party_ids, access_group_list, cxCardholder):
        c = Cardholder()
        c.id = cxCardholder["id"]
        c.authorised = cxCardholder["authorised"]
        c.division = cxCardholder["division"]["href"].split("/")[-1]

        if "@Unique Identity - mq/mqx number" in cxCardholder:
            unique_id = cxCardholder["@Unique Identity - mq/mqx number"].lower()
            c.unique_id = unique_id
            if unique_id in party_ids:
                c.db_party_id = party_ids[unique_id]

        if "@One ID" in cxCardholder:
            c.one_id = cxCardholder["@One ID"]

        if "@Party ID" in cxCardholder:
            c.party_id = cxCardholder["@Party ID"]

        if "firstName" in cxCardholder:
            c.first_name = cxCardholder["firstName"]

        if "lastName" in cxCardholder:
            c.last_name = cxCardholder["lastName"]

        if "description" in cxCardholder:
            c.description = cxCardholder["description"]

        if "lastSuccessfulAccessTime" in cxCardholder:
            c.last_successful_access_time = datetime.strptime(
                cxCardholder["lastSuccessfulAccessTime"], "%Y-%m-%dT%H:%M:%SZ")

        if "lastSuccessfulAccessZone" in cxCardholder and "name" in cxCardholder["lastSuccessfulAccessZone"]:
            c.last_successful_access_zone = cxCardholder["lastSuccessfulAccessZone"]["name"]

        if "accessGroups" in cxCardholder:
            ag_list = []
            for access_group in cxCardholder["accessGroups"]:
                access_group_id = access_group["accessGroup"]["href"].split(
                    "/")[-1]
                if access_group_id in access_group_list:
                    ag = access_group_list[access_group_id]
                    if ag.id not in ag_list:
                        ag_list.append(ag.id)
                        ag.name = access_group["accessGroup"]["name"]
                        c.access_groups.append(ag)

        if "cards" in cxCardholder:
            for card in cxCardholder["cards"]:
                cd = Card()
                cd.id = card["href"].split("/")[-1]
                cd.issue_level = card["issueLevel"] if "issueLevel" in card else 99
                cd.number = card["number"]
                cd.status = card["status"]["type"]
                cd.card_type = card["type"]["name"]
                c.cards.append(cd)

        return c

    def make_event_group(self, cxEventGroup):
        e = EventGroup()
        e.id = cxEventGroup["id"]
        e.name = cxEventGroup["name"]
        for event_type in cxEventGroup["eventTypes"]:
            t = EventType()
            t.id = event_type["id"]
            t.name = event_type["name"]
            e.event_types.append(t)

        return e

    def make_event(self, cxEvent):
        e = Event()
        e.id = cxEvent["id"]
        e.card_facility_code = cxEvent["card"]["facilityCode"]
        e.card_number = cxEvent["card"]["number"]
        e.cardholder_id = cxEvent["cardholder"]["id"] if "cardholder" in cxEvent and "id" in cxEvent["cardholder"] else 0
        if "entryAccessZone" in cxEvent:
            e.entry_access_zone = cxEvent["entryAccessZone"]["id"]
        if "exitAccessZone" in cxEvent:
            e.exit_access_zone = cxEvent["exitAccessZone"]["id"]
        e.door_id = cxEvent["source"]["id"]
        e.event_time = datetime.strptime(cxEvent["time"], "%Y-%m-%dT%H:%M:%SZ")
        e.event_type = cxEvent["type"]["id"]

        return e

    def update(self, entities, Entity):
        session = self.Session()
        results = session.query(Entity).options(joinedload('*'))
        results.merge_result(entities)
        session.commit()
