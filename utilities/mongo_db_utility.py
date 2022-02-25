import pymongo
from utilities.logger_utility import LogUtility


class MongoDbUtility:
    connection_str = "mongodb+srv://mongo_root:mongoDbAcc@fsdscluster0.vskdd.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"
    database_name = "carbon_nano_tubes"

    @staticmethod
    def get_all_entries(collection_name):
        try:
            client = MongoDbUtility.__get_client()
            db = MongoDbUtility.__access_db(client)
            collection = db[collection_name]
            result = list(collection.find())
            MongoDbUtility.__free_up_resources(client=client, db=db, collection=collection)
            return result
        except Exception as ex:
            LogUtility.write_error('db-processing.log',
                                   "Exception occurred, while getting  list of documents of the collection -{collection_name}- (->on MongoDbUtility=>get_all_entries()<-) : {ex}".format(
                                       ex=str(ex), collection_name=collection_name))
            raise Exception('could not able to get list of documents of the collection')

    @staticmethod
    def get_entries_by_condition(collection_name, condition_dict):
        try:
            if type(condition_dict) != dict:
                raise Exception('dictionary type condition param required')
            client = MongoDbUtility.__get_client()
            db = MongoDbUtility.__access_db(client)
            collection = db[collection_name]
            result = list(collection.find(condition_dict))
            MongoDbUtility.__free_up_resources(client=client, db=db, collection=collection)
            return result
        except Exception as ex:
            LogUtility.write_error('db-processing.log',
                                   "Exception occurred, while getting  list of documents of the collection -{collection_name}- by the given condition/s (->on MongoDbUtility=>get_all_entries()<-) : {ex}".format(
                                       ex=str(ex), collection_name=collection_name))
            raise Exception('could not able to get list of documents of the collection by the condition/s')

    @staticmethod
    def insert_entry(collection_name, list_of_document):
        try:
            if type(list_of_document) not in [dict, list]:
                raise Exception('insert_entry only accepts either list or dictionary')
            list_of_document = list_of_document if type(list_of_document) == list else [list_of_document]
            client = MongoDbUtility.__get_client()
            db = MongoDbUtility.__access_db(client)
            collection = db[collection_name]
            collection.insert_many(list_of_document)
            MongoDbUtility.__free_up_resources(client=client, db=db, collection=collection)
        except Exception as ex:
            LogUtility.write_error('db-processing.log',
                                   "Exception occurred, while inserting list of documents to the collection -{collection_name}- (->on MongoDbUtility=>insert_entry()<-) : {ex}".format(
                                       ex=str(ex), collection_name=collection_name))
            raise Exception('could not able to insert list of documents')

    @staticmethod
    def update_entry(collection_name, condition_dict, new_dict):
        try:
            if type(condition_dict) not in [dict] or type(new_dict) not in [dict]:
                raise Exception(
                    'insert_entry only accepts dictionary params for condition dictionary and new dictionary')
            client = MongoDbUtility.__get_client()
            db = MongoDbUtility.__access_db(client)
            collection = db[collection_name]
            collection.update_many(condition_dict, new_dict)
            MongoDbUtility.__free_up_resources(client=client, db=db, collection=collection)
        except Exception as ex:
            LogUtility.write_error('db-processing.log',
                                   "Exception occurred, while updating entries on document of the collection -{collection_name}- (->on MongoDbUtility=>update_entry()<-) : {ex}".format(
                                       ex=str(ex), collection_name=collection_name))
            raise Exception('could not able to update document on {}'.format(collection_name))

    @staticmethod
    def delete_entry(collection_name, condition_dict):
        try:
            if type(condition_dict) not in [dict]:
                raise Exception('insert_entry only accepts dictionary params for condition dictionary')
            client = MongoDbUtility.__get_client()
            db = MongoDbUtility.__access_db(client)
            collection = db[collection_name]
            collection.delete_many(condition_dict)
        except Exception as ex:
            LogUtility.write_error('db-processing.log',
                                   "Exception occurred, while deleting entries on document of the collection -{collection_name}- (->on MongoDbUtility=>delete_entry()<-) : {ex}".format(
                                       ex=str(ex), collection_name=collection_name))
            raise Exception('could not able to update document on {}'.format(collection_name))

    @classmethod
    def __get_client(cls):
        try:
            client = pymongo.MongoClient(cls.connection_str)
            connection_test = client.test
            return client
        except Exception as ex:
            LogUtility.write_error('db-processing.log',
                                   "Exception occurred, while connecting database (->on MongoDbUtility=>__get_client()<-) " + str(
                                       ex))
            raise Exception('could not able to connect mongo-db')

    @classmethod
    def __access_db(cls, client):
        try:
            return client[cls.database_name]
        except Exception as ex:
            LogUtility.write_error('db-processing.log',
                                   "Exception occurred, while getting access of  database (->on MongoDbUtility=>__access_db()<-) " + str(
                                       ex))
            raise Exception(
                'could not able to get access of {db_name}'.format(db_name=cls.database_name))

    @staticmethod
    def __free_up_resources(**resources):
        try:
            for resource_key in resources:
                if resource_key == 'client':
                    resources[resource_key].close()
                    continue
                resources[resource_key] = None
        except Exception as ex:
            LogUtility.write_error('db-processing.log',
                                   "Exception occurred, while closing resources - (->on MongoDbUtility=>__free_up_resources()<-) : {ex}".format(
                                       ex=str(ex)))
            raise Exception('could not able to insert list of documents')
