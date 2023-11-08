"""
GNUploader - GTFS NEO4J Uploader - V1.0 - GPL3 licensed

Author: Matěj Konopík, with love from Brno, Czech Republic
Contact: matejkonopik@gmail.com, xkonop03@vutbr.cz, terrorgarten on GitHub

!!! IMPORTANT !!!
Current version only supports the following GTFS files:
- stops.txt
- routes.txt
- stop_times.txt
- trips.txt
- agency.txt

INFO:

This script serves as a GTFS (General Transit Feed Specification) NEO4J Uploader. It enables the automatic parsing of 
GTFS .ZIP files, extracting the data, and uploading it as a graph to a Neo4j database. It performs all necessary 
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
vary based on the size of data; for instance, it could take approximately 3 hours for 300k nodes (on my old think-pad).

For a complete load, you can instantiate the GNUploader class with the zip_file_path and execute the import with the 
'execute()' method: Uploader = GNUploader(zip_file_path, username, password, neo4j_service_uri) Uploader.execute()

The 'execute()' method sequentially imports all required GTFS data files and establishes relationships as per the 
GTFS specification in the Neo4j graph database.

Please refer to the individual methods within this script for details on the specific data file imports and their 
relationships."""

from py2neo import Graph, Relationship, Node, ClientError
from time import time

import argparse
import zipfile
import tempfile
import os
import shutil
import csv

class GNUploader(object):
    # file type of the GTFS dataset standard
    gtfs_file_extension = ".txt"

    # Constraint queries - py2neo doesn't seem to work, it uses ON notation when FOR notation
    # should be used on current version. Queries are written i nCypher language.
    trip_constraint_query = "CREATE CONSTRAINT FOR (t:Trip) REQUIRE t.trip_id IS UNIQUE"
    route_constraint_query = "CREATE CONSTRAINT FOR (r:Route) REQUIRE r.route_id IS UNIQUE"
    agency_constraint_query = "CREATE CONSTRAINT FOR (a:Agency) REQUIRE a.agency_id IS UNIQUE"
    stop_constraint_query = "CREATE CONSTRAINT FOR (s:Stop) REQUIRE s.stop_id IS UNIQUE"
    trip_index_query = "CREATE INDEX FOR :Trip(service_id)"
    stop_times_index_query = "CREATE INDEX FOR :Stop_times(stop_sequence)"
    stop_index_query = "CREATE INDEX FOR :Stop(name)"

    connect_stop_sequences_query = ("match (s1:Stop_times)-[:PART_OF_TRIP]->(t:Trip), (s2:Stop_times)-["
                                    ":PART_OF_TRIP]->(t) where s2.stop_sequence=s1.stop_sequence+1 create (s1)-["
                                    ":PRECEDES]->(s2);")

    def __init__(self,
                 gtfs_zip_path: str,
                 username: str,
                 password: str,
                 neo4j_service_uri: str,
                 csv_delim: str = ","):
        """
        GTFS NEO4J Uploader class. Serves for creating Uploader objects that are used to automatically parse the input GTFS .ZIP file,
        extract the data and upload them as graph to neo4j. Automatically performs all graph connections accordingly to the
        GTFS specifications (https://developers.google.com/transit/gtfs/reference).
        Currently only uses stops, routes, stop_times, trips and agencies. Expect quite a long runtime, for 300k nodes
        is the runtime about 3hrs.
        Uses py2neo.
        Full load example:
            Uploader = GNUploader(zip_file_path)
            Uploader.execute_import
        For individual import of new data, you use the same method, as the import is input data driven. No redundant imports
        will be performed. Note that the necessary files (stops, routes, stop_times, trips and agencies) still have to
        be present, even if empty.
        :param gtfs_zip_path: Path to the GTFS .ZIP file
        :param username: Username for the neo4j database
        :param password: Password for the neo4j database
        :param neo4j_service_uri: URI of the neo4j database
        :param csv_delim: CSV delimiter for the GTFS files - defaults to ","
        """
        self.username = username
        self.password = password
        self.neo4j_service_uri = neo4j_service_uri
        self.csv_delim = csv_delim
        self.time_format = "%H:%M:%S"
        self.node_ctr = 0
        self.relationship_ctr = 0
        # load input files
        self.gtfs_zip_path = gtfs_zip_path
        self.gtfs_tmp_path = tempfile.mkdtemp()
        self.__extract_zip()
        self.stops = os.path.join(self.gtfs_tmp_path, "stops" + self.gtfs_file_extension)
        self.routes = os.path.join(self.gtfs_tmp_path, "routes" + self.gtfs_file_extension)
        self.stop_times = os.path.join(self.gtfs_tmp_path, "stop_times" + self.gtfs_file_extension)
        self.trips = os.path.join(self.gtfs_tmp_path, "trips" + self.gtfs_file_extension)
        self.agencies = os.path.join(self.gtfs_tmp_path, "agency" + self.gtfs_file_extension)
        self.__validate_gtfs_files_in_dir()
        # connect to neo4j service
        self.client: Graph = self.__connect_to_neo4j()

    def __del__(self):
        shutil.rmtree(self.gtfs_tmp_path)
        # py2neo Graph connection closes automatically

    def __connect_to_neo4j(self) -> Graph:
        """
        Performs connection to the neo4j service. Exits on failure.
        :return: py2neo Graph object
        """
        try:
            graph = Graph(self.neo4j_service_uri, user=self.username, password=self.password)
            print(f"Successfully connected to {self.neo4j_service_uri} as {self.username}")
            return graph
        except Exception as e:
            exit(f"Could not connect to the neo4j database on {self.neo4j_service_uri} - error message: {str(e)}")

    def __extract_zip(self) -> str:
        """
        Extracts the zip file given by GNUploader.gtfs_zip_path
        :return: Extracted zip directory (system temporary dir)
        """
        try:
            # create temporary directory
            # extract archive
            with zipfile.ZipFile(self.gtfs_zip_path, 'r') as zip_archive:
                zip_archive.extractall(self.gtfs_tmp_path)
                print(f"Files extracted into {self.gtfs_tmp_path} successfully!")
            # return path to archive
            return self.gtfs_tmp_path
        # catch general error
        except Exception as e:
            print(f"ERROR: Could not extract archive {self.gtfs_zip_path}. Error message: {str(e)}")
            return ""

    def __validate_gtfs_files_in_dir(self):
        """
        Validates the presence of extracted GTFS files in the temporary directory.
        :return:
        """
        # validate the file existence and exit execution if it doesn't exist
        if not os.path.isfile(self.stop_times):
            exit("Could not find stop_times.txt file. Aborting")
        if not os.path.isfile(self.stops):
            exit("Could not find stops.txt file. Aborting")
        if not os.path.isfile(self.trips):
            exit("Could not find trips.txt file. Aborting")
        if not os.path.isfile(self.routes):
            exit("Could not find routes.txt file. Aborting")
        if not os.path.isfile(self.agencies):
            exit("Could not find agency.txt file. Aborting")

    def execute(self) -> None:
        """
        Main method - executes the import via calling the partial object methods.
        :return: None
        """
        start_time = time()

        # create metadata
        self.__create_constraints_and_indexes()
        print("Indexes created.")
        # main loading sequence
        self.__import_agencies()
        print(f"Agencies imported.")
        self.__import_routes()
        print(f"Routes imported.")
        self.__import_trips()
        print(f"Trips imported.")
        self.__import_stops()
        print(f"Stops imported.")
        self.__import_stop_times()
        print(f"Stop_times imported.")
        self.__connect_stop_times_sequences()
        print("Connected stop_times sequences.")
        end_time = time()
        runtime_seconds = end_time - start_time
        runtime_seconds = f"{runtime_seconds / 60} minutes" if runtime_seconds <= 60 else f"{runtime_seconds} seconds"
        print(
            f"Import complete, took {runtime_seconds} for {self.node_ctr} nodes and {self.relationship_ctr} edges "
            f"imported.")

    def __create_constraints_and_indexes(self) -> None:
        """
        Creates necessary constraints and indexes for the neo4j database. Skips if the constraint already exists.
        :return: None
        """
        # create constraints
        print("Creating constraints and indexes..")
        try:
            self.client.schema.run(self.trip_constraint_query)
        except ClientError:
            print("Agency constraint already exists")
        try:
            self.client.run(self.route_constraint_query)
        except ClientError:
            print("Route constraint already exists.")

        try:
            self.client.run(self.agency_constraint_query)
        except ClientError:
            print("Agency constraint already exists.")

        try:
            self.client.run(self.stop_constraint_query)
        except ClientError:
            print("Stop constraint already exists.")

        # Create indexes
        try:
            self.client.run(self.trip_index_query)
        except ClientError:
            print("Trip index already exists.")

        try:
            self.client.run(self.stop_times_index_query)
        except ClientError:
            print("Stop_times index already exists.")

        try:
            self.client.run(self.stop_index_query)
        except ClientError:
            print("Stop index already exists.")

    def __import_agencies(self):
        column_indices = {}
        with open(self.agencies, 'r', newline='', encoding="utf8") as file:
            # Create a CSV reader
            csv_reader = csv.reader(file)

            # Read the first row to determine column names and skip over blank chars (encoding byte order)
            header_row = next(csv_reader)
            header_row = [s.replace("\ufeff", "") for s in header_row]

            # Populate the column_indices dictionary with column names and their indices
            for index, column_name in enumerate(header_row):
                column_indices[column_name] = index

            # Iterate through the remaining rows in the CSV file
            for row in csv_reader:
                # Access values by column name using the column_indices dictionary
                agency_id = self.get_data(row, column_indices, "agency_id", int)
                agency_name = self.get_data(row, column_indices, "agency_name")
                agency_url = self.get_data(row, column_indices, "agency_url")
                agency_timezone = self.get_data(row, column_indices, "agency_timezone")
                agency_lang = self.get_data(row, column_indices, "agency_lang")
                agency_phone = self.get_data(row, column_indices, "agency_phone")

                agency_node = Node("Agency",
                                   agency_id=agency_id,
                                   name=agency_name,
                                   url=agency_url,
                                   timezone=agency_timezone,
                                   agency_lang=agency_lang,
                                   agency_phone=agency_phone)
                self.client.create(agency_node)

    def __import_routes(self) -> None:
        """
        Imports routes from the routes.txt file and creates necessary relationship connections.
        :return: None
        """
        column_indices = {}
        with open(self.routes, 'r', newline='', encoding="utf8") as file:
            # Create a CSV reader
            csv_reader = csv.reader(file)

            # Read the first row to determine column names and skip over blank chars (encoding byte order)
            header_row = next(csv_reader)
            header_row = [s.replace("\ufeff", "") for s in header_row]

            # Populate the column_indices dictionary with column names and their indices
            for index, column_name in enumerate(header_row):
                column_indices[column_name] = index

            # Iterate through the remaining rows in the CSV file
            for row in csv_reader:
                route_id = self.get_data(row, column_indices, "route_id")
                route_short_name = self.get_data(row, column_indices, "route_short_name")
                route_long_name = self.get_data(row, column_indices, "route_long_name")
                route_type = self.get_data(row, column_indices, "route_type", int)
                agency_id = self.get_data(row, column_indices, "agency_id", int)
                route_node = Node("Route",
                                  route_id=route_id,
                                  short_name=route_short_name,
                                  long_name=route_long_name,
                                  type=route_type)
                self.create_node(route_node)
                agency_node = self.client.nodes.match("Agency", agency_id=agency_id).first()
                self.create_relationship(agency_node, "OPERATES", route_node)

    def __import_trips(self) -> None:
        """
        Imports the trips from the trips.txt file and creates necessary relationship connections.
        :return: None
        """
        column_indices = {}
        with open(self.trips, 'r', newline='', encoding="utf8") as file:
            # Create a CSV reader
            csv_reader = csv.reader(file)

            # Read the first row to determine column names and skip over blank chars (encoding byte order)
            header_row = next(csv_reader)
            header_row = [s.replace("\ufeff", "") for s in header_row]

            # Populate the column_indices dictionary with column names and their indices
            for index, column_name in enumerate(header_row):
                column_indices[column_name] = index
            # Iterate through the remaining rows in the CSV file
            for row in csv_reader:
                route_id = self.get_data(row, column_indices, "route_id")
                service_id = self.get_data(row, column_indices, "service_id", int)
                trip_id = self.get_data(row, column_indices, "trip_id", int)
                trip_headsign = self.get_data(row, column_indices, "trip_headsign")
                wheelchair_accessible = bool(self.get_data(row, column_indices, "wheelchair_accessible"))
                block_id = self.get_data(row, column_indices, "block_id")
                direction_id = self.get_data(row, column_indices, "direction_id", int)
                exceptional = self.get_data(row, column_indices, "exceptional", bool)

                trip_node = Node("Trip",
                                 trip_id=trip_id,
                                 route_id=route_id,
                                 service_id=service_id,
                                 trip_headsing=trip_headsign,
                                 wheelchair_accessible=wheelchair_accessible,
                                 block_id=block_id,
                                 direction_id=direction_id,
                                 exceptional=exceptional)

                self.create_node(trip_node)
                route_node = self.client.nodes.match("Route", route_id=route_id).first()
                self.create_relationship(route_node, "USES", trip_node)

    def __import_stops(self):
        """
        Imports the stops from the stops.txt file and creates the relationship connections.
        :return: None
        """
        column_indices = {}
        with open(self.stops, 'r', newline='', encoding="utf8") as file:
            # Create a CSV reader
            csv_reader = csv.reader(file)

            # Read the first row to determine column names and skip over blank chars (encoding byte order)
            header_row = next(csv_reader)
            header_row = [s.replace("\ufeff", "") for s in header_row]

            # Populate the column_indices dictionary with column names and their indices
            for index, column_name in enumerate(header_row):
                column_indices[column_name] = index
            # Iterate through the remaining rows in the CSV file
            for row in csv_reader:
                stop_id = self.get_data(row, column_indices, "stop_id")
                stop_name = self.get_data(row, column_indices, "stop_name")
                stop_lat = self.get_data(row, column_indices, "stop_lat", float)
                stop_lon = self.get_data(row, column_indices, "stop_lon", float)
                zone_id = self.get_data(row, column_indices, "zone_id", int)
                location_type = self.get_data(row, column_indices, "location_type")
                parent_station = self.get_data(row, column_indices, "parent_station")
                wheelchair_boarding = self.get_data(row, column_indices, "wheelchair_boarding", int)
                platform_code = self.get_data(row, column_indices, "platform_code")

                stop_node = Node("Stop",
                                 stop_id=stop_id,
                                 stop_name=stop_name,
                                 stop_lat=stop_lat,
                                 stop_lon=stop_lon,
                                 zone_id=zone_id,
                                 location_type=location_type,
                                 parent_station=parent_station,
                                 wheelchair_boarding=wheelchair_boarding,
                                 platform_code=platform_code)

                self.create_node(stop_node)

            for row in csv_reader:
                current_stop_id = self.get_data(row, column_indices, "stop_id")
                parent_station = self.get_data(row, column_indices, "parent_station")
                if parent_station and current_stop_id:
                    parent_node = self.client.nodes.match("Stop", stop_id=parent_station).first()
                    child_node = self.client.nodes.match("Stop", stop_id=current_stop_id).first()
                    self.create_relationship(child_node, "PART OF", parent_node)
                else:
                    print(f"Error: Could not load the parent_station {parent_station} of stop {current_stop_id}")

    def __import_stop_times(self) -> None:
        """
        Imports the stop times from the stop_times.txt file and creates the relationship connections.
        :return: None
        """
        column_indices = {}
        with open(self.stop_times, 'r', newline='', encoding="utf8") as file:
            # Create a CSV reader
            csv_reader = csv.reader(file)

            # Read the first row to determine column names and skip over blank chars (encoding byte order)
            header_row = next(csv_reader)
            header_row = [s.replace("\ufeff", "") for s in header_row]

            # Populate the column_indices dictionary with column names and their indices
            for index, column_name in enumerate(header_row):
                column_indices[column_name] = index
            # Iterate through the remaining rows in the CSV file
            for row in csv_reader:
                trip_id = self.get_data(row, column_indices, "trip_id")
                arrival_time = self.get_data(row, column_indices, "arrival_time")
                departure_time = self.get_data(row, column_indices, "departure_time")
                stop_id = self.get_data(row, column_indices, "stop_id")
                stop_sequence = self.get_data(row, column_indices, "stop_sequence")
                pickup_type = self.get_data(row, column_indices, "pickup_type")
                drop_off_type = self.get_data(row, column_indices, "drop_off_type")

                stop_times_node = Node("Stop_times",
                                       trip_id=trip_id,
                                       arrival_time=arrival_time,
                                       departure_time=departure_time,
                                       stop_id=stop_id,
                                       stop_sequence=stop_sequence,
                                       pickup_type=pickup_type,
                                       drop_odd_type=drop_off_type)

                self.create_node(stop_times_node)

                print("trip id: ", trip_id)
                # dynamic_stop_times_node = self.client.nodes.match("Stop_times", stop_sequence=stop_sequence)
                q = "match (t:Trip {trip_id: " + trip_id + "}), (s:Stop {stop_id: '" + stop_id + "'}) \
                create (t)<-[:PART_OF_TRIP]-(st:Stop_times {arrival_time: '" + arrival_time + "', \
                 departure_time: '" + departure_time + "', stop_sequence: " + stop_sequence + "})-[:LOCATED_AT]->(s);"
                self.client.run(q)

    def create_node(self, node: Node) -> None:
        """
        uses py2neo library to create a new Node in the neo4j database from the input py2neo.Node object.
        :param node: The py2neo.Node object to create
        :return: None
        """
        self.client.create(node)
        self.node_ctr += 1
        print(f"Entities: {self.node_ctr + self.relationship_ctr}", end="\r", flush=True)

    def create_relationship(self, from_node: Node, relationship_name: str, to_node: Node) -> None:
        """
        Uses py2neo library to create a new Relationship in the neo4j database from the input py2neo.Node objects
        :param from_node: The "from" node
        :param relationship_name: Relationship label
        :param to_node: The "to" node
        :return: None
        """
        try:
            self.client.create(Relationship(from_node, relationship_name, to_node))
            self.relationship_ctr += 1
            print(f"Entities: {self.node_ctr + self.relationship_ctr}", end="\r", flush=True)
        except Exception as e:
            print(
                f"Error: failed to create relationship ({from_node}, {relationship_name}, {to_node}). Skipping. Error "
                f"message: {e}")

    @staticmethod
    def get_data(row: [str], column_indices, key: str, read_type: type = str):
        """
        Loads data from the row of GTFS file. Uses column_indices and key to dynamically select the corresponding column.
        Converts the data to given read_type type.
        :param row: Input row of GTFS file
        :param column_indices: column index datastructure
        :param key: column key
        :param read_type: datatype to return
        :return: the read value
        """
        try:
            # Try to convert the value to the specified data type
            result = read_type(row[column_indices[key]])
            return result
        except ValueError as e:
            print(f"Error when trying to convert type {read_type} of {key}: {e}\n\t (row: {row})")
            # If a ValueError occurs, return 0 of the specified data type
            try:
                return str(row[column_indices[key]])
            except ValueError:
                return "0"

    def __connect_stop_times_sequences(self):
        try:
            self.client.run(self.connect_stop_sequences_query)
        except Exception as e:
            print(f"Error when connecting stop_times sequences: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="GTFS NEO4J Uploader")
    parser.add_argument("gtfs_zip_path", type=str, help="Path to the GTFS .ZIP file")
    parser.add_argument("username", type=str, help="Username for the Neo4j database")
    parser.add_argument("password", type=str, help="Password for the Neo4j database")
    parser.add_argument("neo4j_service_uri", type=str, help="URI of the Neo4j database")
    parser.add_argument("--csv_delim", type=str, default=",", help="CSV delimiter for the GTFS files (default is ',')")

    args = parser.parse_args()

    uploader = GNUploader(
        args.gtfs_zip_path,
        args.username,
        args.password,
        args.neo4j_service_uri,
        args.csv_delim
    )

    uploader.execute()
