'''
The aim is to store in Redis the final
dataframe containing the computed risks for
each city. The df will be stored in form of
document. It is possible to query the document
for all cities or one city.
'''
import redis.exceptions

from BDT_code.redis_configuration.redis_operations import RedIngestion
from BDT_code.redis_configuration.data_conversion import DataPreparation
from BDT_code.connectors.weather import filepath
from datetime import datetime

# Starting location lookup.
class Lookup:
    def __init__(self):

        '''
        Wraps some methods from the classes defined in the scripts
        in the ‘redis_configuration’ folder in order to let a user
        finally perform a lookup operation. This final class is the
        only one that is actually called by running the ‘_main_.py’ script
        '''

        self.data_prep = DataPreparation()
        self.data_prep.prepare_data()

        self.red_ingestion = RedIngestion()
        data_json = self.data_prep.df_to_dict()
        self.red_ingestion.store_data(data_json)

        self.list_of_risks = set()  # it's a set as to avoid doubles in the risk list
        self.result = None

        # A list of riskiest locations is being built.
        self.summary = None
        self.summarise()

        city_demo = "To obtain city-specific results, please insert one location.\n" + \
                    "Find out from the documentation which cities are available but not list below.\n" + \
                    "\tExamples: Birmingham, Cambridge, Cardiff, London"
        print(city_demo)
        self.lookup(self.insert_location())

    @staticmethod
    def insert_location():
        user_input = input("Enter a city: ")
        return user_input

    def summarise(self):
        self.summary = self.red_ingestion.retrieve_all_location_data()
        print(self.summary)

    def lookup(self, loc):

        try:
            self.result = self.red_ingestion.retrieve_location_data(loc)
            # TO DO: by using geofunctions in Redis, a radius of the user's CHOICE could be selected to show up
            # next to what was requested by the user: if I look up for a city, I'd think that close cities experience
            # similar risks, so our program should at least act accordingly.
            for key, value in self.result.items():
                to_avoid = ['city', 'altitude', 'longitude', 'latitude']
                if key not in to_avoid and value:  # this prints when value == True and key is not 'city' (not needed)
                    self.list_of_risks.add(human_readable[key])

            if "; ".join(self.list_of_risks) == "solar radiation":
                # during the writing of this code, so many cities only had "high uv" risk. We had to do this.
                print("Bring sunscreen!")

            print(f"The risks for {self.result['city']} are: " + '; '.join(self.list_of_risks))

            self.more_lookup(input("Lookup another location? Use Y for yes, or N to exit the program.\n").lower().strip())

        except redis.exceptions.ResponseError:
            print(
                f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} (ERROR) No data found for {loc.capitalize()}" +\
                "\n :(")
            self.more_lookup(input("Lookup another location? Use Y for yes, or N to exit the program.\n").lower().strip())

    def more_lookup(self, another_input):

        if another_input == 'y' and len(another_input) == 1:
            self.lookup(self.insert_location())
        elif another_input == 'n' and len(another_input) == 1:
            print("Remember to close your Redis server; goodbye!\n" + \
                  "Closing...")
            exit()
        else:
            self.more_lookup(input("Please indicate a valid response; Y for yes, and N for no.\n").lower().strip())

# the following is to translate variable names into human-readable names for risks
human_readable = {'min_temp': 'minimum temperature',
                  'max_temp': 'maximum temperature',
                  'radiations': 'solar radiation',
                  'wind_kmh': 'wind gusts above 50 km/h',
                  'hr': 'high humidity',
                  'prec': 'high levels of precipitation'}
