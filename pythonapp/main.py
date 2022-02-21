import os
import pandas as pd
from pymongo import MongoClient

def insert_into_db(db, file_path):
    """
    Loads data file at file_path into MongoDB

    Parameters
    ----------
        db : pymongo.database.Database
            first name of the person
        file : str
            File path of the data file
    """
    # Extract collection name from file name
    collection_name = file_path.split("/")[-1].split(".")[0]
    file_type = file_path.split(".")[-1]

    # Select collection. Drop previous if exists
    collection = db[collection_name]
    collection.drop()

    # Read into pandas dataframe depending on file type
    if file_type == "csv":
        df = pd.read_csv(file_path)
    elif file_type == "xlsx":
        df = pd.read_excel(file_path)
    else:
        return
    df.reset_index(inplace=True)

    # Insert data into collection
    collection.insert_many(df.to_dict(orient='records'))

def example_join_one(db):
    db.create_collection('characters_sheet', viewOn='marvel_characters_info', pipeline=[
        {
            '$lookup': {
                'from': 'charcters_stats', 
                'localField': 'Name', 
                'foreignField': 'Name', 
                'as': 'Stats'
            }
        }, {
            '$lookup': {
                'from': 'superheroes_power_matrix', 
                'localField': 'Name', 
                'foreignField': 'Name', 
                'as': 'Powers'
            }
        }
    ])

def example_join_two(db):
    db.create_collection('comics_and_characters', viewOn='comics', pipeline=[
        {
            '$lookup': {
                'from': 'charactersToComics',
                'localField': 'comicID',
                'foreignField': 'comicID',
                'as': 'characters'
            }
        }, {
            '$lookup': {
                'from': 'characters',
                'localField': 'characters.characterID',
                'foreignField': 'characterID',
                'as': 'characters'
            }
        }
    ])

def main():
    DATA_DIR = "./data"
    MONGODB_URI = 'mongodb://{username}:{password}@{hostname}:27017/'.format(username=os.environ['MONGODB_USERNAME'], password=os.environ['MONGODB_PASSWORD'], hostname=os.environ['MONGODB_HOSTNAME']) # MongoDB connection string

    # Connect to MongoDB
    client = MongoClient(MONGODB_URI)
    db = client["marvel_db"]

    # Load data into MongoDB
    file_list = ['charcters_stats.csv', 'superheroes_power_matrix.csv', 'charactersToComics.csv', 'characters.csv', 'marvel_characters_info.csv', 'comics.csv', 'marvel_dc_characters.xlsx']
    for file_name in file_list:
        insert_into_db(db, os.path.join(DATA_DIR, file_name))

    # Example joins on MongoDB
    example_join_one(db)
    example_join_two(db)

    client.close()

if __name__ == "__main__":
    main()
