import      requests
from        time import sleep
import      random
from        multiprocessing import Process
import      boto3
import      json
from        sqlalchemy import text, create_engine
from        datetime import datetime, date, time


random.seed(100)

'''PULLS DATA FROM TARGET LOCATION'''
class       AWSDBConnector:

        def __init__(self):

            self.HOST =         "pinterestdbreadonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com"
            self.USER =         'project_user'
            self.PASSWORD =     ':t%;yCY3Yjg'
            self.DATABASE =     'pinterest_data'
            self.PORT =         3306
            
        def create_db_connector(self):
            engine =            create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
            return engine


new_connector = AWSDBConnector()

'''ASSORT DATE-TIME OBJECTS (USED FOR JSON FILE)'''

def datetime_handler(obj):
    if isinstance(obj, (datetime, date, time)):
        return str(obj)




'''SEND DATA TO THE KINESIS STREAMS'''
def put_to_api (invoke_url, result, YourStreamName, PartitionKey):
    print('result: :\t', result)  

    payload     =       json.dumps  ({  "StreamName": YourStreamName,
                                        "Data": [{"value": result}],
                                        "PartitionKey": PartitionKey                                            
                                        },  default=datetime_handler)


    headers     =       {'Content-Type': 'application/json'}    
    response    =       requests.request(method="PUT", url=invoke_url, headers=headers, data=payload)
    print(response.status_code)



'''MAIN - INFINITE FUNCTION LOOP'''
def run_infinite_post_data_loop():
            while True:
                sleep(random.randrange(0, 2))
                random_row = random.randint(0, 11000)
                engine = new_connector.create_db_connector()

                with engine.connect() as connection:

                    pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
                    pin_selected_row = connection.execute(pin_string)
                    for row in pin_selected_row:
                        pin_result = dict(row._mapping)

                    geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
                    geo_selected_row = connection.execute(geo_string)
                    for row in geo_selected_row:
                        geo_result = dict(row._mapping)

                    user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
                    user_selected_row = connection.execute(user_string)
                    for row in user_selected_row:
                        user_result = dict(row._mapping)
                    
                    
                    put_to_api("https://moyj7yazp4.execute-api.us-east-1.amazonaws.com/pdp_stream/streams/streaming-0e2a0bfcc015-pin/record", pin_result, "streaming-0e2a0bfcc015-pin", "pin_partition")
                    put_to_api("https://moyj7yazp4.execute-api.us-east-1.amazonaws.com/pdp_stream/streams/streaming-0e2a0bfcc015-geo/record", geo_result, "streaming-0e2a0bfcc015-geo", "geo_partition")
                    put_to_api("https://moyj7yazp4.execute-api.us-east-1.amazonaws.com/pdp_stream/streams/streaming-0e2a0bfcc015-user/record", user_result, "streaming-0e2a0bfcc015-user", "user_partition")







if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')