import json

from BDT_code.connectors.weather import meteo_connector
from BDT_code.data_model.meteo_spark import Spark_session

class DataPreparation:
    def __init__(self, connector_path, spark_config):
        self.connector_path = connector_path
        self.spark_config = spark_config
        self.diz = None
        self.df = None

    def prepare_data(self):
        self.fetch_data()
        self.process_data()

    def fetch_data(self):
        my_connector = meteo_connector(self.connector_path)
        self.diz = my_connector.info_dict()

    def process_data(self):
        session = Spark_session()
        session.start_session()

        rows = session.from_dict_to_rows(self.diz)
        modified_rows = session.modify_rows(rows)
        self.df = session.final_df(modified_rows)

    def df_to_dict(self):
        res = {}
        json_str = self.df.toJSON().collect()  # list of JSON strings
        for my_json in json_str:
            json_dict = json.loads(my_json)
            res[json_dict['city']] = json_dict
        return res

