import sqlalchemy as db
from datetime import datetime
from sqlalchemy import func
from sqlalchemy.orm import sessionmaker, joinedload
from cardaxdb_model import BaseCardax, Cardholder, AccessGroup, Card, BaseDatabank, Patron, CardOneID, UnicardCard, AccessZone, Door, Event, EventGroup, EventType


query_events = """
select /*+ parallel(4) */
       e.id,
       to_char(e.event_time, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') as event_time,
       e.card_number,
       e.card_facility_code,
       e.cardholder_id,
       ch.first_name || ' ' || ch.last_name as cardholder_name,
       eg.id as event_group_id,
       eg.name as event_group_name,
       et.id as event_type_id,
       et.name as event_type_name,
       e.door_id,
       d.name as door_name,
       enaz.id as entry_access_zone_id,
       enaz.name as entry_access_zone_name,
       exaz.id as exit_access_zone_id,
       exaz.name as exit_access_zone_name,
       to_char(e.event_time, 'WW') as event_time_week_year,
       to_char(e.event_time, 'W') as event_time_week_month,
       to_char(e.event_time, 'D') as event_time_day_week,
       to_char(e.event_time, 'HH24:"00:00Z"') as event_time_hour,
       to_char(e.event_time, 'MI') as event_time_minute
  from event e
  join cardholder ch on ch.id = e.cardholder_id
  join card c on c.card_number = e.card_number
  join event_type et on et.id = e.event_type_id
  join event_group eg on eg.id = et.event_group_id
  join door d on d.id = e.door_id
  left join access_zone enaz on enaz.id = e.entry_access_zone
  left join access_zone exaz on exaz.id = e.entry_access_zone
 where e.id > :pos
"""


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

    def get_events(self, pos=0):
        conn = self.engine.connect()

        ResultProxy = conn.execute(query_events, pos=pos)
        result = ResultProxy.fetchall()

        events = []
        for row in result:
            e = {}
            e["id"] = row[0]
            e["event_time"] = row[1]
            e["card_number"] = row[2]
            e["card_facility_code"] = row[3]
            e["cardholder_id"] = row[4]
            e["cardholder_name"] = row[5]
            e["event_group_id"] = row[6]
            e["event_group_name"] = row[7]
            e["event_type_id"] = row[8]
            e["event_type_name"] = row[9]
            e["door_id"] = row[10]
            e["door_name"] = row[11]
            e["entry_access_zone_id"] = row[12]
            e["entry_access_zone_name"] = row[13]
            e["exit_access_zone_id"] = row[14]
            e["exit_access_zone_name"] = row[15]
            e["week_of_year"] = int(row[16])
            e["week_of_month"] = int(row[17])
            e["day_of_week"] = int(row[18])
            e["hour"] = row[19]
            e["minute"] = int(row[20])

            events.append(e)

        conn.close()

        return events

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
        e.id = int(cxEvent["id"])
        e.card_facility_code = cxEvent["card"]["facilityCode"]
        e.card_number = int(cxEvent["card"]["number"])
        e.cardholder_id = int(cxEvent["cardholder"]["id"]) if "cardholder" in cxEvent and "id" in cxEvent["cardholder"] else 0
        if "entryAccessZone" in cxEvent:
            e.entry_access_zone = int(cxEvent["entryAccessZone"]["id"])
        if "exitAccessZone" in cxEvent:
            e.exit_access_zone = int(cxEvent["exitAccessZone"]["id"])
        e.door_id = int(cxEvent["source"]["id"])
        e.event_time = datetime.strptime(cxEvent["time"], "%Y-%m-%dT%H:%M:%SZ")
        e.event_type = EventType(id=cxEvent["type"]["id"], name=cxEvent["type"]["name"])

        return e

    def update(self, entities, Entity):
        session = self.Session()
        results = session.query(Entity).options(joinedload('*'))
        results.merge_result(entities)
        session.commit()
