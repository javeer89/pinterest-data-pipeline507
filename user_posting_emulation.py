import      requests
from        time import sleep
import      random
from        multiprocessing import Process
import      boto3
import      json
import      sqlalchemy
from        sqlalchemy import text


random.seed(100)


class       AWSDBConnector:

        def __init__(self):

            self.HOST =         "pinterestdbreadonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com"
            self.USER =         'project_user'
            self.PASSWORD =     ':t%;yCY3Yjg'
            self.DATABASE =     'pinterest_data'
            self.PORT =         3306
            
        def create_db_connector(self):
            engine =            sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
            return engine


new_connector = AWSDBConnector()

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
                    
                    print(pin_result)
                    print(geo_result)
                    print(user_result)


def post_data_to_api (invoke_url: str, result: dict):

    headers =       {'Content-Type': 'application/vnd.kafka.json.v2+json'}    
    payload =       json.dumps({
                            "StreamName": "Post_Data",
                            "Data":     [{   #Data should be send as pairs of column_name:value, with different columns separated by commas
                                            "value": result
                                        }]
                                    })

    response = requests.request("POST", invoke_url, headers=headers, data=payload)
    print(response.status_code)





if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')




'''

def post_data_to_api (method, invoke_url, data):

    invoke_url_pin =    "https://moyj7yazp4.execute-api.us-east-1.amazonaws.com/pinDP/topics/0e2a0bfcc015.pin"
    invoke_url_geo =    "https://moyj7yazp4.execute-api.us-east-1.amazonaws.com/pinDP/topics/0e2a0bfcc015.geo"
    invoke_url_user =   "https://moyj7yazp4.execute-api.us-east-1.amazonaws.com/pinDP/topics/0e2a0bfcc015.user"
    
    dataframe =     run_infinite_post_data_loop()
    
    pin_df =        run_infinite_post_data_loop().pin_result
    geo_df =        run_infinite_post_data_loop()
    user_df =       run_infinite_post_data_loop()
    
    headers =       {'Content-Type': 'application/vnd.kafka.json.v2+json'}
    
    {'index': 7528, 'unique_id': 'fbe53c66-3442-4773-b19e-d3ec6f54dddf', 'title': 'No Title Data Available', 'description': 'No description available Story format', 'poster_name': 'User Info Error', 'follower_count': 'User Info Error', 'tag_list': 'N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e', 'is_image_or_video': 'multi-video(story page format)', 'image_src': 'Image src error.', 'downloaded': 0, 'save_location': 'Local save in /data/mens-fashion', 'category': 'mens-fashion'}
    {'ind': 7528, 'timestamp': datetime.datetime(2020, 8, 28, 3, 52, 47), 'latitude': -89.9787, 'longitude': -173.293, 'country': 'Albania'}
    {'ind': 7528, 'first_name': 'Abigail', 'last_name': 'Ali', 'age': 20, 'date_joined': datetime.datetime(2015, 10, 24, 11, 23, 51)}

    {"index": example_df["index"], "name": example_df["name"], "age": example_df["age"], "role": example_df["role"]}

    payload_pin =       json.dumps({
                            "StreamName": "Post_Data",
                            "Data":     [{   #Data should be send as pairs of column_name:value, with different columns separated by commas
                                            "value": dataframe.pin_result
                                        }]
                                    })
    
    payload_geo =       json.dumps({
                            "StreamName": "Geolocation_Data",
                            "Data":     [{   #Data should be send as pairs of column_name:value, with different columns separated by commas
                                            "value": dataframe.geo_result
                                        }]
                                    })
    
    payload_user =      json.dumps({
                            "StreamName": "User Data",
                            "Data":     [{   #Data should be send as pairs of column_name:value, with different columns separated by commas
                                            "value": dataframe.user_result
                                        }]
                                    })
    
    response = requests.request("POST", invoke_url_pin,     headers=headers,    data=payload_pin)
    response = requests.request("POST", invoke_url_geo,     headers=headers,    data=payload_geo)
    response = requests.request("POST", invoke_url_user,    headers=headers,    data=payload_user)
    
    print(response.status_code)


'''