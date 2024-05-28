import json
import config
import logging
import multiprocessing as MP
import urllib3
import requests
from requests.exceptions import HTTPError
from sqlalchemy import create_engine, text

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logging.basicConfig(format="[%(asctime)s][%(levelname)s]: %(message)s")
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

query_ids = """
select id, unique_id_original, db_party_id
  from cardholder
 where unique_id is not null
   and party_id is null
   and db_party_id is not null
 order by unique_id
"""

baseurl = config.cardax_baseurl
session = requests.Session()
session.headers = {
    "Authorization": config.cardax_apikey,
    "Content-Type": "application/json"
}
# session.proxies = {"https": "http://127.0.0.1:8888", "https": "http://127.0.0.1:8888"}
cardaxdb = create_engine(config.cardaxdb_conn)


def patch_cardholder_ids(id, unique_id, party_id):
    data = {
        "@One ID": unique_id,
        "@Party ID": party_id
    }
    log.info("%s: %s", id, data)
    try:
        r = session.patch(f"{baseurl}/cardholders/{id}", data=json.dumps(data), verify=False)
        r.raise_for_status()
    except HTTPError as e:
        log.error("error: %s", e)
        raise Exception(f"HTTP error occurred: {e}")


if __name__ == "__main__":
    log.info("starting")
    conn = cardaxdb.connect()
    pool = MP.Pool(processes=10)

    result = conn.execute(text(query_ids))
    pool.starmap(patch_cardholder_ids, result)

    pool.close()
    conn.close()
    log.info("complete")
