from pymongo import MongoClient
from bson.objectid import ObjectId

class AnimalShelter(object):
    """ CRUD operations for Animal collection in MongoDB """

    def __init__(self, username, password):
        # Initializing the MongoClient. This helps to 
        # access the MongoDB databases and collections.
        # This is hard-wired to use the aac database, the 
        # animals collection, and the aac user.
        # Definitions of the connection string variables are
        # unique to the individual Apporto environment.
        #
        # Connection Variables
        #
        USER = username
        PASS = password
        HOST = 'nv-desktop-services.apporto.com'
        PORT = 30969
        DB = 'aac'
        COL = 'animals'
        #
        # Initialize Connection
        #
        self.client = MongoClient('mongodb://%s:%s@%s:%d' % (USER,PASS,HOST,PORT))
        self.database = self.client['%s' % (DB)]
        self.collection = self.database['%s' % (COL)]

    # Method to implement the C in CRUD.
    def create(self, data):
        if data is not None:
            doc_id = self.database.animals.insert_one(data).inserted_id  # data should be dictionary
            if doc_id:
                print("Insertion successful!")
                return True
            else:
                print("Error with insertion...")
                return False
        else:
            raise Exception("Nothing to save, because data parameter is empty")

    # Method to implement the R in CRUD.
    def read(self, data):
        if data is not None:
            results = list(self.database.animals.find(data))
            
            if len(results) == 0:
                print("No results retrieved")
            else:
                return results
        else:
            raise Exception("Nothing to save, because data parameter is empty")
            
    # Method to implement the U in CRUD.
    def update(self, read_data, update_data):
        if read_data is not None or update_data is not None:
            return self.database.animals.update_one(read_data, { "$set": update_data }).modified_count
        else:
            raise Exception("Nothing to save, because read_data or update_data parameter is empty")
            
    # Method to implement the D in CRUD.
    def delete(self, delete_data):
        if delete_data is not None:
            return self.database.animals.delete_one(delete_data).deleted_count
        else:
            raise Exception("Nothing to save, because delete_data parameter is empty")

    # Method to implement the Water Rescue filter.
    def water_rescue_filter(self):
        results = list(self.database.animals.find({
            "$and":[{
                "$or":[
                    {"breed": 'Chesa Bay Retr Mix' },
                    { "breed": 'Newfoundland Mix' },
                    { "breed": 'Labrador Retriever' }
                ]},
                { "sex_upon_outcome": 'Intact Female'},
                { "age_upon_outcome_in_weeks": { "$gte": 26, "$lte": 156 }
                }
            ]
        }))
        
        if len(results) == 0:
            print("No results retrieved")
        else:
            return results
        
    # Method to implement the Mountain or Wilderness Rescue filter.
    def mountain_wilderness_rescue_filter(self):
        results = list(self.database.animals.find({
            "$and":[{
                "$or":[
                    { "breed": 'German Shepherd' },
                    { "breed": 'Alaskan Malamute' },
                    { "breed": 'Old English Sheepdog' },
                    { "breed": 'Siberian Husky' },
                    { "breed": 'Rottweiler' }
                ]},
                { "sex_upon_outcome": 'Intact Male'},
                { "age_upon_outcome_in_weeks": { "$gte": 26, "$lte": 156 }
                }
            ]
        }))

        if len(results) == 0:
            print("No results retrieved")
        else:
            return results
        
    # Method to implement the Disaster or Individual Rescue filter.
    def disaster_individual_rescue_filter(self):
        results = list(self.database.animals.find({
            "$and":[{
                "$or":[
                    { "breed": 'Doberman Pinscher' },
                    { "breed": 'German Shepherd' },
                    { "breed": 'Golden Retriever' },
                    { "breed": 'Bloodhound' },
                    { "breed": 'Rottweiler' }
                ]},
                { "sex_upon_outcome": 'Intact Male'},
                { "age_upon_outcome_in_weeks": { "$gte": 30, "$lte": 300 }
                }
            ]
        }))

        if len(results) == 0:
            print("No results retrieved")
        else:
            return results
