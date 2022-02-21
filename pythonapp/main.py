import os
import pandas as pd
from pymongo import MongoClient

def read_file_into_df(file_path):
    """
    Reads csv/xlsx file into Pandas DataFrame

    Parameters
    ----------
    file_path : str
        Path to input file
        
    Returns
    -------
    pandas.DataFrame
        Dataframe containing data from input file
    """
    # Read into pandas dataframe depending on file type
    file_type = file_path.split(".")[-1]
    if file_type == "csv":
        df = pd.read_csv(file_path)
    elif file_type == "xlsx":
        df = pd.read_excel(file_path)
    else:
        return
    df.reset_index(inplace=True)

    return df

def insert_df_into_mongo(db, collection_name, df):
    """
    Loads Pandas DataFrame into MongoDB collection

    Parameters
    ----------
    db : pymongo.database.Database
        MongoDB database
    collection_name : str
        Collection to insert to
    df : pandas.DataFrame
        DataFrame containing data to be inserted
    """
    # Select collection. Drop previous if exists
    collection = db[collection_name]
    collection.drop()

    # Insert data into collection
    collection.insert_many(df.to_dict(orient='records'))

def read_mongo_to_df(db, collection_name, query={}):
    """
    Reads collection from MongoDB into Pandas dataframe

    Parameters
    ----------
    db : pymongo.database.Database
        MongoDB database
    collection_name : str
        Collection to read from
    query: str
        MongoDB query

    Returns
    -------
    pandas.DataFrame
        Dataframe containing data from MongoDB collection
    """
    cursor = db[collection_name].find(query)

    # Expand the cursor and construct the DataFrame
    df =  pd.DataFrame(list(cursor))

    # Delete the _id
    del df['_id']
    
    del df['index']

    return df

def example_join_one(db):
    VIEW_NAME = 'characters_sheet_view'
    # Drop collection
    db[VIEW_NAME].drop()

    # Create view
    db.create_collection(VIEW_NAME, viewOn='marvel_characters_info', pipeline=[
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
    VIEW_NAME = 'comics_and_characters_view'
    # Drop collection
    db[VIEW_NAME].drop()

    # Create view
    db.create_collection(VIEW_NAME, viewOn='comics', pipeline=[
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

def example_join_three(db):
    VIEW_NAME = 'characters_and_comics_view'
    # Drop collection
    db[VIEW_NAME].drop()

    # Create view
    db.create_collection(VIEW_NAME, viewOn='characters', pipeline=[
        {
            '$lookup': {
                'from': 'charactersToComics',
                'localField': 'characterID',
                'foreignField': 'characterID',
                'as': 'comics'
            }
        }, {
            '$lookup': {
                'from': 'comics',
                'localField': 'comics.comicID',
                'foreignField': 'comicID',
                'as': 'comics'
            }
        }
    ])

def example_join_one_pandas(db):
    # Read from MongoDB collection into Pandas dataframe
    characters_df = read_mongo_to_df(db, 'characters')
    comics_df = read_mongo_to_df(db, 'comics')
    charactersToComics_df = read_mongo_to_df(db, 'charactersToComics')

    # Quick stats on number of unique comics and characters
    print("******************************")
    print("{} comics without characters".format(len(set(comics_df['comicID'].unique()) - set(charactersToComics_df['comicID'].unique()))))
    print("{} characters without comics".format(len(set(characters_df['characterID'].unique()) - set(charactersToComics_df['characterID'].unique()))))

    print("Missing info on {} comics".format(len(set(charactersToComics_df['comicID'].unique()) - set(comics_df['comicID'].unique()))))
    print("Missing info on {} characters".format(len(set(charactersToComics_df['characterID'].unique()) - set(characters_df['characterID'].unique()))))
    print("******************************")

    # Find comics and their characters
    print("******************************")
    comicsCharacters_df = pd.merge(comics_df, charactersToComics_df, on='comicID', how='left')
    comicsCharacters_df = pd.merge(comicsCharacters_df, characters_df, on='characterID', how='left')
    print("Comics and their characters")
    print(comicsCharacters_df)
    print("******************************")
    insert_df_into_mongo(db, "comics_and_characters", comicsCharacters_df)

    # Find characters and their comics
    print("******************************")
    charactersComics_df = pd.merge(characters_df, charactersToComics_df, on='characterID', how='left')
    charactersComics_df = pd.merge(charactersComics_df, comics_df, on='comicID', how='left')
    print("Characters and their comics")
    print(charactersComics_df)
    print("******************************")
    insert_df_into_mongo(db, "characters_and_comics", charactersComics_df)

def example_join_two_pandas(db):
    # Read from MongoDB collection into Pandas dataframe
    superheroes_power_df = read_mongo_to_df(db, 'superheroes_power_matrix')
    characters_stats_df = read_mongo_to_df(db, 'charcters_stats')
    characters_info_df = read_mongo_to_df(db, 'marvel_characters_info')

    # Find characters with their info, stats and powers
    characters_sheet_df = pd.merge(characters_info_df, characters_stats_df, on='Name', how='outer')
    characters_sheet_df = pd.merge(characters_sheet_df, superheroes_power_df, on='Name', how='outer')
    print("******************************")
    print("Characters with their info, stats and powers")
    print(characters_sheet_df)
    print("******************************")
    insert_df_into_mongo(db, "characters_sheet", characters_sheet_df)


def main():
    DATA_DIR = "./data"
    MONGODB_URI = 'mongodb://{username}:{password}@{hostname}:27017/'.format(username=os.environ['MONGODB_USERNAME'], password=os.environ['MONGODB_PASSWORD'], hostname=os.environ['MONGODB_HOSTNAME']) # MongoDB connection string
    file_list = ['charcters_stats.csv', 'superheroes_power_matrix.csv', 'charactersToComics.csv', 'characters.csv', 'marvel_characters_info.csv', 'comics.csv', 'marvel_dc_characters.xlsx']

    # Connect to MongoDB
    client = MongoClient(MONGODB_URI)
    db = client["marvel_db"]

    # Load data into MongoDB
    for file_name in file_list:
        file_path = os.path.join(DATA_DIR, file_name)

        # Read file into Pandas DataFrame
        df = read_file_into_df(file_path)
        
        # Extract collection name from file name
        collection_name = file_path.split("/")[-1].split(".")[0]
        
        # Export from Pandas DataFrame to MongoDB collection
        insert_df_into_mongo(db, collection_name, df)

    # Example joins with Pandas
    example_join_one_pandas(db) # Joins for comics and characters
    example_join_two_pandas(db) # Full outer join for character sheets

    # Example joins on MongoDB
    example_join_one(db) # Character sheets
    example_join_two(db) # Comics and their characters
    example_join_three(db) # Characters and their comics

    # Close connection to MongoDB
    client.close()

if __name__ == "__main__":
    main()
