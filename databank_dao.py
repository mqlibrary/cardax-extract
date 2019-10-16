import sqlalchemy as db
from sqlalchemy.orm import sessionmaker
from cardaxdb_model import BaseDatabank, Patron, UnicardCard, CardOneID

query_patrons = """
select /*+ parallel(4) */ p.source_system, lower(p.oneid) as oneid, p.party_id, p.given_name, p.family_name, s.facultyname
  from library_ro.vw_patron_details p
  join library_ro.vw_patron_details_staff s on p.identity_bk = s.staffid
 where p.source_system <> 'ELC'
union
select /*+ parallel(4) */ p.source_system, lower(p.oneid) as oneid, p.party_id, p.given_name, p.family_name, s.org_unit_name
  from library_ro.vw_patron_details p
  join library_ro.vw_patron_details_student s on p.identity_bk = s.student_bk
 where p.source_system <> 'ELC'
union
select /*+ parallel(4) */ p.source_system, lower(p.oneid) as oneid, p.party_id, p.given_name, p.family_name, replace(s.patron_stat_cat, 'PatronCat.', '')
  from library_ro.vw_patron_details p
  join library_ro.vw_patron_details_sponsor s on p.hub_identity_sk = s.hub_identity_sk
 where p.source_system <> 'ELC'
"""

query_unicard = """
with cards as (
select p.given_names, p.surname, p.card_type, p.party_id, lower(p.one_id1) as one_id1,
       lower(p.one_id2) as one_id2, c.intserial, c.barcode, c.ac_num, c.strprintreason,
       row_number() over(partition by p.party_id order by c.intserial desc) as rank
  from ods_unicard.tblpersons p
  join ods_unicard.tblcards c on c.intpersonid=p.universe_id)
select intserial, given_names, surname, card_type, party_id, one_id1,
       one_id2, barcode, ac_num, strprintreason
  from cards
 where rank = 1
"""

query_party_ids = """
select distinct party_id, lower(oneid) as one_id
  from mq_vault_aspect.asp_identity
 where oneid is not null
"""


class DatabankDAO:
    def __init__(self, engine):
        self.engine = engine
        Session = sessionmaker(bind=self.engine, expire_on_commit=False)
        self.session = Session()

    def initialise_schema(self):
        BaseDatabank.metadata.drop_all(self.engine)
        BaseDatabank.metadata.create_all(self.engine)
        BaseDatabank.metadata.bind = self.engine

    def get_databank_patrons(self):
        conn = self.engine.connect()

        ResultProxy = conn.execute(query_patrons)
        result = ResultProxy.fetchall()

        oneids = []
        patrons = []
        for row in result:
            if row[1] in oneids:
                continue
            oneids.append(row[1])

            p = Patron()
            p.source_system = row[0]
            p.one_id = row[1]
            p.party_id = row[2]
            p.first_name = row[3]
            p.last_name = row[4]
            p.faculty = row[5]
            patrons.append(p)

        conn.close()

        return patrons

    def get_unicard_cards(self):
        conn = self.engine.connect()

        ResultProxy = conn.execute(query_unicard)
        result = ResultProxy.fetchall()

        cards = []
        for row in result:
            c = UnicardCard()
            c.intserial = row[0]
            c.first_name = row[1]
            c.last_name = row[2]
            c.card_type = row[3]
            c.party_id = row[4]
            c.barcode = row[7]
            c.ac_num = row[8]
            c.print_reason = row[9]

            if row[5] is not None:
                one_id = CardOneID()
                one_id.intserial = row[0]
                one_id.one_id = row[5]
                one_id.card = c
                c.one_ids.append(one_id)

            if row[6] is not None:
                one_id = CardOneID()
                one_id.intserial = row[0]
                one_id.one_id = row[6]
                one_id.card = c
                c.one_ids.append(one_id)

            cards.append(c)

        conn.close()

        return cards

    def get_party_ids(self):
        conn = self.engine.connect()

        ResultProxy = conn.execute(query_party_ids)
        result = ResultProxy.fetchall()

        party_ids = {}
        for row in result:
            party_ids[row[1]] = row[0]

        conn.close()

        return party_ids
