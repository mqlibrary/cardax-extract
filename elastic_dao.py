import json
import requests


class ElasticDAO:
    def __init__(self, url, usr, pwd, idx, estype):
        self.session = requests.Session()
        self.session.auth = (usr, pwd)
        self.session.headers = {"Content-Type": "application/json"}
        # self.session.proxies = {"https": "http://127.0.0.1:8888"}
        self.baseurl = url
        self.index = idx
        self.estype = estype

    def save_events(self, events):
        pattern = '{{"create": {{ "_index": "{}", "_type": "{}", "_id": {}}}}}'
        data = ""
        for event in events:
            event["id"] = int(event["id"])
            data = data + pattern.format(self.index, self.estype, int(event["id"])) + "\n" + json.dumps(event) + "\n"

        r = self.session.post(self.baseurl + "/_bulk", data=data)

    def get_max_pos(self):
        url = "{}/{}/{}/_search".format(self.baseurl, self.index, self.estype)
        query = '{"size": 0, "aggs": {"max_id": {"max": { "field": "id"}}}}'
        r = self.session.post(url, data=query)
        result = r.json()
        if "aggregations" in result and "max_id" in result["aggregations"] \
                and "value" in result["aggregations"]["max_id"] \
                and result["aggregations"]["max_id"]["value"] is not None:
            return int(result["aggregations"]["max_id"]["value"])

        return 0


if __name__ == "__main__":
    import config
    es = ElasticDAO(config.elastic_url, config.elastic_usr, config.elastic_pwd, "cardax-events", "event")
    print(es.get_max_pos())
