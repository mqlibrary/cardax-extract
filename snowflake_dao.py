import sqlalchemy as db
from sqlalchemy.orm import sessionmaker
from cardaxdb_model import BaseSnowflake, Patron, UnicardCard, CardOneID, Faculty

query_patrons = """
select /*+ parallel(4) */ p.source_system, lower(p.oneid) as oneid, p.party_id, p.given_name, p.family_name, s.facultyname, s.positiontype, s.departmentname
  from vw_patron_details p
  join vw_patron_details_staff s on p.identity_bk = s.staffid
 where p.is_deleted = 'N'
   and s.is_deleted = 'N'
   and p.source_system <> 'ELC'
   and p.oneid is not null
union
select /*+ parallel(4) */ p.source_system, lower(p.oneid) as oneid, p.party_id, p.given_name, p.family_name, s.org_unit_nm, s.cs_cat_lvl_cd, s.cs_cat_type_desc
  from vw_patron_details p
  join vw_patron_details_student s on p.identity_bk = s.student_bk
 where p.is_deleted = 'N'
   and s.is_deleted = 'N'
   and p.source_system <> 'ELC'
   and p.oneid is not null
union
select /*+ parallel(4) */ p.source_system, lower(p.oneid) as oneid, p.party_id, p.given_name, p.family_name, 'Sponsored', replace(replace(s.patron_stat_cat, 'PatronCat.', ''), 'PatronCat_', ''), replace(replace(s.patron_stat_cat2, 'PatronCat.', ''), 'PatronCat_', '')
  from vw_patron_details p
  join vw_patron_details_sponsor s on p.IDENTITY_BK  = s.oneid
 where p.is_deleted = 'N'
   and s.is_deleted = 'N'
   and p.source_system <> 'ELC'
   and p.oneid is not null
"""

query_unicard = """
with barcodes as (
  select p.universe_id,
         p.party_id,
         lower(p.one_id1) as one_id1,
         lower(p.one_id2) as one_id2,
         p.given_names,
         p.surname,
         c.card_type,
         c.intserial,
         c.barcode,
         c.ac_num,
         c.dteissuedate,
         c.strprintreason,
         i.person_image_url,
         i.dtecapturedate,
         row_number() over (partition by p.party_id
                            order by c.intserial desc) as rank
    from tblpersons p
    join tblcards c on p.universe_id = c.intpersonid
    join tblpersonimages i on p.universe_id = i.universe_id  
   where p.party_id is not null)
select b.universe_id,
       b.party_id,
       b.one_id1,
       b.one_id2,
       b.given_names,
       b.surname,
       b.card_type,
       b.intserial,
       b.barcode,
       b.ac_num,
       b.dteissuedate as update_time,
       b.strprintreason,
       b.person_image_url,
       b.dtecapturedate as image_capture_time
  from barcodes b
 where b.rank = 1
 order by update_time desc nulls last, b.party_id
"""

query_faculties = """
select 'mq' || lower(identity_bk) as one_id, faculty_name, start_date
  from input__employee_position
union
(with faculties as (
select lower(ca.student_bk) as student_bk,
       oua.org_unit_nm,
       ca.cs_strt_date as cs_start_dt,
       row_number() over (partition by ca.student_bk order by
           case co.cs_cat_lvl_cd
               when 'PG' then 1
               when 'UG' then 2
               else 3
               end,
           ca.cs_cr_val desc,
           ca.cs_strt_date desc, ca.cs_app_date desc,
           oua.effct_dt desc, oua.org_unit_nm, oua.org_unit_type_cd) as rank
  from stage__course_admission as ca
  join stage__course_offering as co on co.course_bk = ca.course_bk and
                                       co.course_session_bk = ca.course_session_bk and
                                       co.offering_year_bk = ca.offering_year_bk and
                                       co.course_offering_bk = ca.course_offering_bk and
                                       co.campus_bk = ca.campus_bk
  join stage__lnk_coffer_to_orgunit as cto on cto.course_bk = ca.course_bk and
                                              cto.course_session_bk = ca.course_session_bk and
                                              cto.offering_year_bk = ca.offering_year_bk and 
                                              cto.course_offering_bk = ca.course_offering_bk and
                                              cto.campus_bk = ca.campus_bk
  join stage__org_unit_amis as oua on cto.org_unit_cd = oua.org_unit_cd
 where oua.EAP_SEARCH_FG = 'N'
   and ((ca.cs_stg_cd = 'ADM' and
         ca.cs_stts_cd in ('ADM', 'LOA', 'POTC'))
    or (ca.cs_stg_cd = 'COMP' and
        ca.cs_stts_cd = 'PASS')))
select student_bk as one_id,
       org_unit_nm,
       cs_start_dt
  from faculties
 where rank = 1)
union
select lower(identity_bk) as one_id, 'Sponsored', expiry_date 
  from input__party_identity
 where source_system = 'SPONSOR'
"""

query_party_ids = """
select distinct party_id, lower(oneid) as one_id
  from vw_patron_details 
 where oneid is not null
"""


class SnowflakeDAO:
    def __init__(self, engine):
        self.engine = engine
        Session = sessionmaker(bind=self.engine, expire_on_commit=False)
        self.session = Session()

    def initialise_schema(self):
        BaseSnowflake.metadata.drop_all(self.engine)
        BaseSnowflake.metadata.create_all(self.engine)
        BaseSnowflake.metadata.bind = self.engine

    def get_snowflake_patrons(self):
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
            p.category = row[6]
            p.dept_degree = row[7]
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

    def get_patron_faculties(self):
        conn = self.engine.connect()

        ResultProxy = conn.execute(query_faculties)
        result = ResultProxy.fetchall()

        faculties = []
        for row in result:
            faculty = Faculty()
            faculty.one_id = row[0]
            faculty.faculty_name = row[1]
            faculties.append(faculty)

        conn.close()

        return faculties
