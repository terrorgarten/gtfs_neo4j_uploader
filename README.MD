# GTFS -> NEO4J Uploader
- Author: Matěj Konopík, with love from Brno, Czech Republic

- Contact: matejkonopik@gmail.com, xkonop03@vutbr.cz, terrorgarten on GitHub

- License: GPLv3

## General information *(the boring stuff)*

This script serves as a GTFS (General Transit Feed Specification) NEO4J Uploader. It enables the automatic parsing of 
GTFS .ZIP files, extracting the data, and uploading it as a graph to an **EMPTY** instance of Neo4j database. It performs all necessary 
graph connections according to the GTFS specifications provided by Google Developers (
https://developers.google.com/transit/gtfs/reference).

The GTFS standard deals with transit schedules, geographic information, and service information for public 
transportation. This code specifically focuses on the 'stops', 'routes', 'stop_times', 'trips', and 'agencies' files 
within the GTFS specification.

Using the py2neo library, this script automates the extraction of data from these GTFS files, loads it into the Neo4j 
graph database, creates nodes and relationships for entities such as agencies, routes, trips, stops, 
and their related information. The main loading sequence ensures the insertion of the data with proper connections 
and relationships.

Note: The code is optimized to process the input GTFS files, create constraints and indexes, import data from 
individual files, and create relationships between the entities within the graph database. The expected runtime can 
vary based on the size of data; for instance, it could still take approximately 3 hours for 300k nodes (on my old think-pad).

For a complete load, you can instantiate the GNUploader class with the zip_file_path and execute the import with the 
'execute()' method: Uploader = GNUploader(zip_file_path, username, password, neo4j_service_uri) Uploader.execute()

The 'execute()' method sequentially imports all required GTFS data files and establishes relationships as per the 
GTFS specification in the Neo4j graph database.

Please refer to the individual methods within this script for details on the specific data file imports and their 
relationships.

## Installation

This script requires **Python 3.6** or higher. It also requires **pip** to deps from the provided requirements.txt file. 
It is crucial to have an empty instance of Neo4j running and providing the script it's valid credentials. If you just want to try this out, running a Neo4j server locally using docker is described in the following section.

To install on linux, follow these steps:

clone this repository:
```bash
git clone https://github.com/terrorgarten/gtfs_neo4j_uploader
```

cd to the folder

```bash
cd gtfs_neo4j_uploader
```

install the required packages using the following command:


```bash
pip install -r requirements.txt
```

### Running Neo4j container locally
I have prepared a `docker-compose.yml` file, you can use it to create a local database for testing. Please note you need to have **docker installed**. Refer to docker download guidelines at https://docs.docker.com/get-docker/.

Firstly, you should adjust the values for auth in the `docker-compose.yml` file so its not something general, even if running locally. These are specifically:
```yml
...
NEO4J_AUTH: <yourname>/<yourpassword>
...
```

Start Docker (if not running) - might vary for different linux distros...
```bash
sudo systemctl start docker
```

Launch the container in detached mode (-d)
```bash
docker-compose up -d
```
Everything should be done automatically for you.
You can now visit the Neo4j browser interface on http://localhost:7474/


## Launching the upload

After installing, you can use the script from the command line to launch the upload:
```bash
python gtfs_neo4j_uploader.py <db username> <db password> <neo4j db url> 
```
You can also use a different delimiter in the GTFS files if needed, according to the following synapsis:
```bash
usage: uploader.py [-h] [--csv_delim CSV_DELIM]
                   gtfs_zip_path username password neo4j_service_uri
```

## Issues
Should you run into any issues with the program, feel free to leave a GitHub issue or email me directly. Any pull requests are welcome!

