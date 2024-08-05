import os
from us_visa_approval.constants.constants import DATABASE_NAME, MONGODG_URL_KEY
import pymongo
import certifi
from us_visa_approval.exception.exception import CustomException
from us_visa_approval.logger.logger import logging
import sys

ca = certifi.where()

class MongoDBClient():
    '''
    Class Name: MongoDBClient
    Description: This class is used to connect to MongoDB and exports the dataframe
    Output: MongoDB connection to database
    On Failure: Raise Exception
    '''
    client = None   

    def __init__(self, database_name=DATABASE_NAME) -> None:
        try:
            if MongoDBClient.client is None:
                mongodb_url = os.getenv(MONGODG_URL_KEY)
                if mongodb_url is None:
                    raise Exception("MongoDB URL not found in environment variables")
                MongoDBClient.client = pymongo.MongoClient(mongodb_url, tlsCAFile=ca)
            self.database = self.client[database_name]
            self.database_name = database_name
            logging.info("MongoDB Connection Successful")
        except Exception as e:
            raise CustomException(e, sys)

