"""
For engaging with GCP BigQuery tables and Cloud Storage
"""

import os
from pathlib import Path
from google.cloud import bigquery, storage, dataform_v1beta1
import subprocess
import pandas_gbq
import pandas as pd
import time
import re
from google.oauth2 import service_account
from dotenv import load_dotenv

## Doorstep Analytics Scripts
from gcp_constants import GCP_BIGQUERY_TABLES, GCP_STORAGE_BUCKETS  ## Private table config, not included in Repo releases
from schemas import bigquery_schemas

from config_logging import setup_logging
logger = setup_logging()
load_dotenv()

credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
project_id = os.getenv("BIGQUERY_PROJECT_ID")
bigquery_client = bigquery.Client.from_service_account_json(credentials_path)
storage_client = storage.Client.from_service_account_json(credentials_path)
credentials = service_account.Credentials.from_service_account_file(credentials_path)


def getLocationsToScrape():
    """
    Fetches the next location to scrape from the BigQuery scrape list table.

    This function queries the configured BigQuery table for locations marked to be scraped.
    It extracts and returns details of the first location record.
    If no locations are returned, the Linux VM power off.

    Returns:
        dict: A dictionary containing:
            - location (str): The name of the location to scrape.
            - country (str): The country associated with the location.
            - currency (str): The currency code for the location.
            - isOverview (bool): Whether the scrape is an overview-level scrape. Not Used in main Code

    Note:
        - Executes a system shutdown command if no results are found.
        - For local systems without GCP access, run get_customLocationsToScrape() instead
    """

    query_string = f""" SELECT Location, Country, Currency, isOverview
            FROM `{GCP_BIGQUERY_TABLES['scrapeList']}` WHERE 1=1
            """
    
    query_job = bigquery_client.query(query_string)
    results = query_job.result()
    
    ## If no outstanding locations to scrape, power off Linux VM
    if results.total_rows == 0:
        logger.info('No scrape to-do list results returned. Powering off')
        subprocess.run(['sudo', 'systemctl', 'poweroff'], check=True)
    
    first_row = next(iter(results), None)
    location = first_row['Location']
    country = first_row['Country']
    currency = first_row['Currency']
    is_overview = first_row['isOverview']

    logger.info(f"Found {results.total_rows} location rows")
    
    return {'location': location,
            'country': country,
            'currency': currency,
            'isOverview': is_overview}

def get_customLocationsToScrape():
    """
    Use this as a custom function with hard-coded variables if not accessing GCP tables

    Returns:
        dict: A dictionary containing:
            - location (str): The name of the location to scrape.
            - country (str): The country associated with the location.
            - currency (str): The currency code for the location.
            - isOverview (bool): False. To be removed
    """

    return {'location': "Berlin",
            'country': "Germany",
            'currency': "EUR",
            'isOverview': False}

class GCPManager:
    """
    Handles all interaction with GCP services, including BigQuery, Cloud Storage and VM commands
    """

    def __init__(self, context):
        self.ctx = context

    def CSVtoBigQuery_runner(self):
        """
        Find the generated Overview csv file, and push the contents to BigQuery
        The Overview csv file is pattern matched, a messy solution
        Data matching the location in the overviewSource table is deleted then re-inserted
        """

        ## Search for generated csv file with 'Overview' in title
        folder = Path(self.ctx.output_folder)
        pattern = f"Airbnb_{self.ctx.location}_{self.ctx.country}_Overview_*.csv"
        csv_file_name = next(folder.glob(pattern), None)

        ## Delete from source table and re-insert from CSV file
        self.Delete_fromBigQuery('overviewSource')
        self.PushCSVtoBigQuery(csv_file_name, 'overviewSource')
    
    def runQuery(self, query_string):
        """
        Return the results of a BigQuery query, written in SQL as query_string
        """
        query_job = bigquery_client.query(query_string)
        results = query_job.result()
        return results
    
    def runQueryToDataFrame(self, query_string):
        """
        Extract data from BigQuery as a Pandas df, using an SQL query in query_string
        """
        query_job = bigquery_client.query(query_string)
        df = query_job.to_dataframe()
        return df
    
    def getMapTiles(self):
        """
        Retrieves map tile boundary coordinates for the current location from BigQuery.

        This method queries the `location_coords` table to obtain the northeast and southwest
        latitude/longitude coordinates and zoom level associated with the current location.

        Returns:
            list[dict]: A list containing a single dictionary with the following keys:
                - neLat (float): Northeast latitude point.
                - neLng (float): Northeast longitude point.
                - swLat (float): Southwest latitude point.
                - swLng (float): Southwest longitude point.
                - zoom (int): Zoom level for map tile scraping or visualization.

        Note: For custom script runs, this function call should be replaced with output from get_customMapTileList()
        """

        results = self.runQuery( f""" SELECT * FROM `doorbll-399214.sj_aircloud.location_coords`
            WHERE Location = '{self.ctx.location}' AND Country = '{self.ctx.country}' LIMIT 1 """ )  
        row = next(results)
        
        return [{'neLat': row['NeLat'], 'neLng': row['NeLng'], 'swLat': row['SwLat'],
                'swLng': row['SwLng'], 'zoom': row['Zoom']}]
    
    def get_customMapTileList(self):
        """
        Alternative Co-Ords extraction from string
        Paste in an Airbnb URL, and extract map tile coordinates.
        """

        ## Load config again for airbnb map search URL
        url = input('Paste the URL for the area you want to search:')

        while True:            
            ## Extract coords using regex
            pattern_coords = r'ne_lat=([-0-9.]+)&ne_lng=([-0-9.]+)&sw_lat=([-0-9.]+)&sw_lng=([-0-9.]+)'
            match_coords = re.search(pattern_coords, url)
            
            ## Extract zoom value using regex
            pattern_zoom = r'zoom=([\d.]+)'
            match_zoom = re.search(pattern_zoom, url)
            
            if match_coords and match_zoom:
                ne_lat, ne_lng, sw_lat, sw_lng = match_coords.groups()
                zoom = match_zoom.group(1)
                mapTile = {
                    "neLat": float(ne_lat),
                    "neLng": float(ne_lng),
                    "swLat": float(sw_lat),
                    "swLng": float(sw_lng),
                    "zoom": int(float(zoom)),
                }
                return [mapTile]
            else:
                ## Loop until a correct URL is generated, containing ne_lat, ne_lng, sw_lat, sw_lng
                logger.warning("URL does not contain co-ordinates. The URL should be in the format \"https://www.airbnb.co.uk/s/\"... \
                                \"&ne_lat=49.51742709579624&ne_lng=8.61274074530948&sw_lat=49.30348332369533&sw_lng=8.324515005491321&zoom=11\"...") 
                url = input('Paste the URL for the area you want to search:')
    
    def Delete_fromBigQuery(self, table):
        """
        Delete all rows from the BigQuery table with the current location
        """
        
        logger.info(f'Deleting from {GCP_BIGQUERY_TABLES[table]} where location == {self.ctx.location}')
        self.runQuery( f""" DELETE FROM `{GCP_BIGQUERY_TABLES[table]}`
            WHERE country = '{self.ctx.country}' and location = '{self.ctx.location}'""" )
        
    def pushArchiveToCloud(self, archive_tar_filename):
        """
        Move the tar.gz file from file_mgr to the archive GCP Cloud Storage bucket
        Not publicly accessible
        """

        logger.info(f'Moving {archive_tar_filename} to Cloud Storage')
        bucket = storage_client.bucket(f"{GCP_STORAGE_BUCKETS['archive']}")
        blob = bucket.blob(f'{archive_tar_filename}')
        blob.upload_from_filename(archive_tar_filename, timeout=1000)
        
    def pushZipToCloud(self, zip_filename, bucket):
        """
        Move the website accessible zip files from file_mgr to the
        relevant GCP Cloud Storage bucket.
        """

        logger.info(f'Pushing {zip_filename} to {GCP_STORAGE_BUCKETS[bucket]}')
        blob = storage_client.bucket(f"{GCP_STORAGE_BUCKETS[bucket]}").blob(os.path.basename(zip_filename))
        with open(zip_filename, 'rb') as f:
            blob.upload_from_file(f, content_type='application/zip', timeout=1000, rewind=True)
    
    def PushCSVtoCloud(self, csv_filename, bucket):
        """
        Move the website accessible csv files from file_mgr to the
        relevant GCP Cloud Storage bucket.
        """

        logger.info(f"Pushing CSV {csv_filename} to {GCP_STORAGE_BUCKETS[bucket]}")
        blob = storage_client.bucket(GCP_STORAGE_BUCKETS[bucket]).blob(os.path.basename(csv_filename))
        with open(csv_filename, 'rb') as csv_file:
            blob.upload_from_file(csv_file, content_type='text/csv')
            
    def PushDataFrameToBigQuery(self, df, table_id, if_exists_action='append'):
        """
        Append data from a Pandas df to a BigQuery table
        Uses the pandas_gbq module
        """

        df.to_gbq(destination_table=GCP_BIGQUERY_TABLES[table_id], credentials=credentials, if_exists=if_exists_action)
       
    def PushCSVtoBigQuery(self, csv_file_name, table_id):
        """
        Takes a csv file and appends contents to a BigQuery table
        """

        ## Apply schema per schemas.py, and ignore csv headers
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            schema=bigquery_schemas[table_id] )
        
        logger.info(f'Pushing CSV {csv_file_name} to {GCP_BIGQUERY_TABLES[table_id]}')
        
        with open(csv_file_name, "rb") as source_file:
            load_job = bigquery_client.load_table_from_file(source_file, GCP_BIGQUERY_TABLES[table_id], job_config=job_config)
        
        try:
            load_job.result()
        except Exception as e:
            logger.error("Load job failed:", e)
            if hasattr(load_job, "errors"):
                for err in load_job.errors:
                    logger.error(err)
        
    def GenerateOverviewDataFrame(self):
        """
        Download Dataform output as pandas df, using pandas_gbq module
        Store df as self.overview_df
        """

        logger.info('Downloading Overview data from GCP as DataFrame')
        
        query = f"""SELECT * FROM `{GCP_BIGQUERY_TABLES['aggregatedTable']}`
                    WHERE Location = '{self.ctx.location}' AND Country = '{self.ctx.country}'"""
        
        df = pandas_gbq.read_gbq(query, project_id=project_id, credentials=credentials)
        df['RecordInserted'] = pd.to_datetime(df['RecordInserted'])
        self.overview_df = df
        
    def pushOverviewDataFrame_toCloudStorage(self):
        """
        Zip the Overview csv file, and push to current folder for website access
        """
        
        df = self.overview_df
        
        ## Push to Overview to current folder for website access
        logger.info('Pushing Overview to Current folder')
        csv_filename = f'{self.ctx.output_folder}/DoorstepAnalytics_{self.ctx.location}_{self.ctx.country}_AirbnbOverview.csv'
        zip_filename = f"{self.ctx.output_folder}/DoorstepAnalytics_{self.ctx.location}_{self.ctx.country}_AirbnbOverview.zip"
        df.to_csv(csv_filename, index=False)
        self.ctx.file_mgr.Zip_CSVfile('Overview', csv_filename, zip_filename)
        self.pushZipToCloud(zip_filename, 'current')
        
        logger.info('Overview CSV uploads complete')

    def wait_for_workflow_completion(self, client, workflow_name, timeout_seconds=600):
        """ Waits for a Dataform workflow invocation to complete.

        Args:
            client: The DataformClient instance.
            workflow_name: The name of the workflow invocation.
            timeout_seconds: The maximum time to wait in seconds.

        Returns:
            The final WorkflowInvocation object.

        Raises:
            TimeoutError: If the workflow does not complete within the timeout.
            GoogleAPIError: If the workflow invocation fails.
        """

        start_time = time.time()
        while time.time() - start_time < timeout_seconds:
            invocation = client.get_workflow_invocation(name=workflow_name)
            state = invocation.state

            if state == dataform_v1beta1.WorkflowInvocation.State.SUCCEEDED:
                logger.info("Workflow invocation SUCCEEDED.")
                return invocation
            elif state == dataform_v1beta1.WorkflowInvocation.State.FAILED:

                ## Retrieve and log detailed error information from the invocation object
                error_details = ""
                if invocation.invocation_timing:
                    for action_result in invocation.invocation_timing.action_timing:
                        if action_result.status.code != 0: ## Status code 0 is OK
                            error_details += (
                                f"\nAction: {action_result.action_name}"
                                f"\nError: {action_result.status.message}"
                            )

                full_error_message = f"Workflow invocation FAILED. Details: {error_details}"
                logger.error(full_error_message)
            elif state == dataform_v1beta1.WorkflowInvocation.State.CANCELLED:
                logger.warning("Workflow invocation CANCELLED.")
                return invocation
            elif state == dataform_v1beta1.WorkflowInvocation.State.RUNNING:
                time.sleep(7)  # Wait for 7 seconds before polling
                logger.info("Workflow is still running, waiting...")
            else:
                logger.info(f"Workflow is in state: {state.name}, waiting...")
                time.sleep(4)

        raise TimeoutError("Timeout waiting for Dataform workflow to complete.")

    def InvokeDataform(self):
        """Triggers a Dataform workflow from a development workspace."""

        client = dataform_v1beta1.DataformClient()
        dataform_parent = os.getenv("DATAFORM_PARENT")
        dataform_workspace = os.getenv("DATAFORM_WORKSPACE")
        workspace_path = f"{dataform_parent}/workspaces/{dataform_workspace}"

        ## 1. Create a CompilationResult from the workspace
        compilation_result = client.create_compilation_result(
            parent=dataform_parent,
            compilation_result=dataform_v1beta1.CompilationResult(
                workspace=workspace_path
            )
        )

        ## 2. Use the CompilationResult name to create a WorkflowInvocation
        workflow_invocation = dataform_v1beta1.WorkflowInvocation(
            compilation_result=compilation_result.name,
            invocation_config=dataform_v1beta1.InvocationConfig()
        )

        ## 3. Trigger the workflow invocation
        response = client.create_workflow_invocation(
            parent=dataform_parent,
            workflow_invocation=workflow_invocation
        )

        logger.info("Started Dataform run for tbl_overview")
        logger.info(f"Workflow ID: {response.name}")

        ## 4. Wait for the workflow to complete
        try:
            
            final_invocation = self.wait_for_workflow_completion(client, response.name)
            logger.info(f"Workflow run completed with state: {final_invocation.state.name}")
            return final_invocation.state.name
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}", exc_info=True)
            return "FAILED"
        
    def UpdateWebsiteTables(self):
        """
        The website uses a quick reference table to get most recently updated datasets
        Update this table from the latest aggregatedTable data
        """

        logger.info("Update most recent website table")
        self.runQuery(f""" CREATE OR REPLACE TABLE `{GCP_BIGQUERY_TABLES['recentlyUpdated']}` AS
            SELECT Country, Location, COUNT(Airbnb_ListingID) AS Airbnb_Listings
            FROM {GCP_BIGQUERY_TABLES['aggregatedTable']}
            GROUP BY Country, Location ORDER BY MAX(RecordInserted) DESC LIMIT 3; """ )
    
    def LogCompletionInBigQuery(self):   
        """
        Log the job completion in BigQuery. Make a simple df and push to BigQuery table
        Note the isOverview field is no longer used
        """

        df_logger = pd.DataFrame({ 'Location': [self.ctx.location], 'Country': [self.ctx.country], 'RecordInserted': [self.ctx.scrape_datetime], 'isOverview': 1 })
        logger.info('Adding log finalized entry')
        pandas_gbq.to_gbq(df_logger, f"{GCP_BIGQUERY_TABLES['logCompleted']}", project_id=project_id, if_exists='append', credentials=credentials)
        
if __name__ == '__main__':
    print("This is the GCP handler. Run web_scraper.py instead")