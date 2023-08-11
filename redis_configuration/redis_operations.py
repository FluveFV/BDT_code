import redis.exceptions
from rejson import Path, Client
from datetime import datetime

class RedIngestion:

    def __init__(self, host="localhost", port=6379):     #check if your port is the same; 6379 is by default.
        '''
        Instantiates a Redis connection and provides methods
        to store data into Redis and perform a lookup operation
        '''
        self.rj = Client(host=host, port=port, decode_responses=True)

    def store_data(self, data: object) -> object:
        '''
        Args:
            data in a json or bit-like object
        '''
        self.rj.jsonset('doc', Path.rootPath(), data) #   this is the location where to store data "$."
        print(f"{datetime.now().strftime('%Y/%m/%d %H:%M:%S')} INFO Data was successfully stored via Redis.")

    def retrieve_location_data(self, city_name):
        '''
        Args:
            city_name:

        Returns: json-like dictionary.
        '''
        # TO IMPROVE
        query = city_name.lower().strip()
        result = self.rj.jsonget('doc', f'.{query}')

        if result:
            return result
        else:
            return None
    def retrieve_all_location_data(self):
        try:
            return self.rj.jsonget('doc','$')
            return self.rj.jsonget('doc', '$[?.('
                                          '@.min_temp == True'
                                          ')]')
        except redis.exceptions.ResponseError as e:
            print(f"{datetime.now().strftime('%Y/%m/%d %H:%M:%S')} (ERROR) There was an issue while building a summary "
                  f"of the risks." + "\n :(")
            print(f"{datetime.now().strftime('%Y/%m/%d %H:%M:%S')} (INFO) {e}")
            # e stores the redis generated error info

            return None


#data_prep = DataPreparation(filepath)
#red_ingestion = RedIngestion()
#data_prep.prepare_data()
#data_json = data_prep.df_to_dict()
#print(type(data_json))
#red_ingestion.store_data(data_json)
#print(red_ingestion.retrieve_location_data('London'))

#{"london":{'city': 'London', 'wind_kmh':1, 'radiation':0}, "birmingham":{'city': 'Birmingham', 'wind_kmh':1, 'radiation':0}}
