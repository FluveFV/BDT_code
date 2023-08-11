import json

from BDT_code.connectors.weather import meteo_connector, filepath
from BDT_code.spark_data_model.spark_storage import Spark_session

class DataPreparation:
    def __init__(self):
        '''
        Wraps methods from the classes defined to fetch data from sources and
        elaborate them in Spark. It also has a method that converts the Spark
        dataframe to a dictionary that will be stored into Redis.
        '''
        self.connector_path = filepath
        self.diz = None
        self.df = None

    def prepare_data(self):
        self.fetch_ids()
        self.process_data()

    def fetch_ids(self):
        self.diz = meteo_connector(self.connector_path).info_dict()

    def process_data(self):
        session = Spark_session()
        session.start_session()
        rows = session.from_dict_to_rows(self.diz)

        modified_rows = session.modify_rows(rows)
        self.df = session.final_df(modified_rows)

    def df_to_dict(self):
        lookable_dictionary = {}
        json_str = self.df.toJSON().collect()
        for element in json_str:
                node = json.loads(element)
                lookable_dictionary[node['city'].lower()] = node
        return lookable_dictionary



#data_prep = DataPreparation(filepath)
#data_prep.prepare_data()
#print(data_prep.df_to_dict())
