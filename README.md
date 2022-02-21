# Marvel Superheroes

Simple set up that does ETL on data from https://www.kaggle.com/dannielr/marvel-superheroes into MongoDB.

### Set up
There are 3 services in the Docker Compose file.
1. mongodb
    - MongoDB
    - Port 27017 is exposed for access via MongoDB Compass
2. mongo-express
    - Web GUI for read access to MongoDB. Note: Does not support MongoDB Views
    - Port 8081 is exposed for access via web browser
3. pythonapp
    - Python application that reads data from the raw files and loads the data into MongoDB
    - pythonapp checks for mongodb to be up before starting

### Run
`
docker-compose up --build
`

Use the following command to check for errors while running the Python application
`
docker logs -f pythonapp
`

### More information on pythonapp
Data to be ingested is stored in ./pythonapp/data
