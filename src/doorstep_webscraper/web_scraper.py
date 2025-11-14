"""
Doorstep Analytics - Airbnb Web Scraper
Written by Simon Salamian for DoorstepAnalytics.com
"""

import re
import os
from datetime import datetime, timedelta
import shutil
import base64
import tomllib
import sys
import subprocess

## Doorstep Analytics Scripts
from constants import URLS, DO_NOT_TRANSLATE
from utils import r_sleep, dict_subset
from config_logging import setup_logging
from file_manager import FileManager
from data_handler import DataHandler
from ml_handler import Neighbourhood
from gcp_manager import GCPManager, getLocationsToScrape
from session_handler import SessionHandler

## Load in .toml file path
toml_path = os.path.join(os.path.dirname(__file__), "../../config.toml")
toml_path = os.path.abspath(toml_path)

## Custom logging script
logger = setup_logging()
            
class Context:
    """
    Context holds fixed location data used across all Doorstep Airbnb Webscraper modules.
    """
    
    def __init__(self):
        self.scrape_datetime = datetime.today()
        self.scrape_date = self.scrape_datetime.date()
        self.scrape_date_str = self.scrape_date.strftime("%Y%m%d")
        self.loadConfig()
        self.loadLocation()
        self.output_folder = f"data/{self.location}"

        self.file_mgr = None
        self.data_handler = None
        self.session = None

    def loadConfig(self):
        """
        Load in configuration options from the TOML file.
        """

        with open(toml_path, "rb") as f:
            config = tomllib.load(f)

        ## Boolean options. If true, the web scraper downloads more data but runs for longer
        self.isWebPreview = config['custom_config']['is_web_preview']
        self.scrapeCalendar = config['custom_config']['scrape_calendar']
        self.scrapePricing = config['custom_config']['scrape_weekly_pricing']
        self.scrapeDescription = config['custom_config']['scrape_description']
        self.translateDescriptionToEnglish = config['custom_config']['translate_description_to_English']
        self.scrapeReviews = config['custom_config']['scrape_reviews']
        self.openCSVonCompletion = config['custom_config']['open_csv_on_completion']

        self.log_on_multiples = 200 if self.isWebPreview else 20

        ## Recommended set to INFO by default
        self.log_level = config['custom_config']['log_level']

    def loadLocation(self):
        location_dict = getLocationsToScrape()
        self.country = location_dict['country']
        self.location = location_dict['location']
        self.currency = location_dict['currency']
        #self.isWebPreview = location_dict['isWebPreview']
        #self.GUID = location_dict['GUID']
        
    def UpdateContextWithHandlers(self, file_mgr, gcp_manager, data_handler, session):
        """
        Share the handlers across the context class, to be accessed anywhere
        """
        self.file_mgr = file_mgr
        self.gcp_manager = gcp_manager
        self.data_handler = data_handler
        self.session = session

class WebScraper:
    def __init__(self, context: Context):
        self.ctx = context
        self.mapTile_init = ctx.gcp_manager.getMapTiles()
        
    def _setup_scraper_context(self, **kwargs):
        """
        Set up Listing ID trackers (downloaded and updated) and search parameters, according to the runner type
        For pricing, a check-in, check-out date and the number of adult staying is added
        """
        if self.runner_type == 'pricing':
            session.check_in = kwargs.get('start_date')
            session.check_out = kwargs.get('end_date')
            session.week_label = kwargs.get('date_label')
            session.adults = kwargs.get('guests')
        
        self.downloaded_listingIDs = file_mgr.listJSONFilesInFolder('overview')
        self.updated_listingIDs = []

        if self.runner_type == 'explore':
            self.preview_mapTileList = []
    
        if self.runner_type == 'stays':
            session.check_in = None
            session.check_out = None
            session.adults = 1
            for data in file_mgr.JSONFileDataGenerator(folder='overview'):
                if dict_subset(data, 'listing_stays') is not None:
                    self.updated_listingIDs.append(data['listing_stays']['id'])
                    
    def _trackDownloaded(self, listing_id):
        """
        Track newly downloaded listing ID. Return True if listing ID is not already saved, else False
        Log every 20/200 new listings, depending on preview mode
        """
        if listing_id not in self.downloaded_listingIDs:
            self.downloaded_listingIDs.append(listing_id)
            if len(self.downloaded_listingIDs) % self.ctx.log_on_multiples == 0:
                logger.info(f"Downloaded {len(self.downloaded_listingIDs)} listings")
            return True
        else:
            logger.debug(f'Listing ID {listing_id} already downloaded, skipping')
        return False
    
    def _trackUpdated(self, listing_id):
        """
        Track newly updated listing IDs, return True if not already updated, and log every 20/200 updates, depending on preview mode.
        """
        if listing_id not in self.updated_listingIDs:
            self.updated_listingIDs.append(listing_id)
            if len(self.updated_listingIDs) % self.ctx.log_on_multiples == 0 and self.runner_type != 'pricing':
                logger.info(f"Updated {len(self.updated_listingIDs)} listings")
            return True
    
    def _fetchInitialTile(self, coords):
        """ Return all API data for the first page of each Map tile only """
        offset = 0
        url = f"{URLS[self.runner_type]}&currency=USD"
        payload = session.createDataPayloadMapAPI(self.runner_type, coords, offset)
        response = session.makeRequest(request_type='post', url=url, dataPayload=payload)
        return response

    def _processTile(self, coords, total_count, initial_response):
        """
        Processes a map tile by fetching and handling listings in pages.

        Iterates through all listings for a given tile using pagination. The first page uses
        the provided `initial_response` to avoid redundant API calls; subsequent pages are
        fetched via the API. Each page returns up to 18 listings.

        For each page:
            - Extracts listing data from the response.
            - If Explore run, updates the Preview_MapTile list, for use in pricing run
            - Logs and saves the response if no listings are found.
            - Processes the extracted listings based on the runner type.

        Args:
            coords (dict): Coordinates of the tile to process (e.g., {'neLat': ..., 'swLat': ..., ...}).
            total_count (int): Total number of listings expected for the tile.
            initial_response (dict): The first API response for this tile to avoid an extra request.
        """
        offset = 0

        ## Build preview MapTile List for first 70 listings in Explore API
        ## This list is used to efficiently get pricing data in the pricing run
        if self.runner_type == 'explore' and len(self.downloaded_listingIDs) <= 70:
            self._updatePreview_mapTiles(coords)

        while offset < total_count:

            ## To avoid repeating requests, only call API on offset page. API already called in _fetchInitialTile
            response = ( initial_response if offset == 0 else
                ctx.session.makeRequest('post',
                    f"{URLS[self.runner_type]}&currency=USD",
                    dataPayload=ctx.session.createDataPayloadMapAPI(self.runner_type, coords, offset)
                )
            )

            ## Extract useful listing data from response. Returns a list of dicts, each item about 1 listing
            listings = self._extractListingsFromResponse(response)
            if not listings:
                logger.debug(f'No useful listing data in response, outputting to debug/no_listing_data.json')
                ctx.file_mgr.saveJSONFile(response, 'debug', 'no_listing_data')
                return

            ## Save listing data according to the runner_type
            self._processListings(listings)

            ## 18 listings returned per page. Iterate through pages (items offset) until total_count reached
            offset += 18
            r_sleep(0.01)
            
    def _processListings(self, listings):
        """
        Processes a list of listings based on the runner type and saves relevant data.

        For each listing in the input:
            - Skips split stays or unsupported listings for 'stays' and 'pricing' runner types.
            - Decodes the listing ID for 'stays' and 'pricing', or extracts it directly for other types.
            - Handles missing listing IDs by logging and saving the raw data for debugging.
            - Performs runner-specific processing:
                - 'pricing': extracts pricing data if the listing is updated or already downloaded.
                - 'explore': processes only previously tracked listings.
                - 'stays': augments existing 'explore' listings or fetches fresh property details
                for new listings, merging with stays API data.
            - Saves calendar, description (with optional translation), and reviews if scraping is enabled.

        Args:
            listings (list[dict]): A list of listing dicts returned from the API.
        """

        ## Where e represent the data for each individual listing
        for e in listings:
            if self.runner_type in ['stays', 'pricing']:

                ## Stays API sometimes returns recommended Split Stays across two listings. These are not helpful, ignore
                if dict_subset(e, 'splitStaysListings') or dict_subset(e, 'demandStayListing') is None:
                    logger.debug("Split stay listing, continuing")
                    continue

                ## Decode the Listing ID from base 64
                try:
                    listing_id_encoded = dict_subset(e, 'demandStayListing', 'id')
                    listing_id_decoded = base64.b64decode(listing_id_encoded).decode("utf-8")
                    listing_id = listing_id_decoded.replace("DemandStayListing:", "")
                except:
                    logger.warning("No listing ID decoded in pricing/stays. Output to debug/no_listing_ID_Decoded.json")
                    ctx.file_mgr.saveJSONFile(e, 'debug', 'no_listing_ID_Decoded')
            
            else:
                listing_id = dict_subset(e, 'listing', 'id')
        
            ## Fail safe in case a listing does not have a listing ID. This should not be possible, unless the Airbnb API is modified
            if not listing_id:
                logger.warning(f'No listing ID in response, outputting to debug/no_listing_ID.json')
                ctx.file_mgr.saveJSONFile(e, 'debug', 'no_listing_ID')
                continue

            ## -------- PRICING ONLY --------
            if self.runner_type == 'pricing':
                if self._trackUpdated(listing_id) or listing_id in self.downloaded_listingIDs:
                    ctx.session.extractPricingToFile(e, listing_id)
                continue

            ## -------- EXPLORE --------
            if self.runner_type == 'explore':
                if not self._trackDownloaded(listing_id):
                    continue
                e = e['listing']

            ## -------- STAYS --------
            elif self.runner_type == 'stays':

                ## Case 1: Augment existing Explore listing sata (existing) with extra stays and basic pricing data (e)
                if (e.get('__typename') == 'StaySearchResult' and 
                    listing_id in self.downloaded_listingIDs and 
                    self._trackUpdated(listing_id)):
                    
                    logger.debug(f"Listing ID {listing_id} already seen in explore API run, augmenting")
                    existing = ctx.file_mgr.readJSONFile('overview', listing_id)

                    e = {**existing, **e}
                    ctx.file_mgr.saveJSONFile(e, 'overview', listing_id)
                    continue

                ## Case 2: Listing not seen before, pull fresh data from stays endpoint
                if self._trackDownloaded(listing_id) and self._trackUpdated(listing_id):
                    logger.debug(f"Listing ID {listing_id} not seen on explore API run")

                    ## Fetch listing detail from Property Detail API, for listing not downloaded in Explore API run
                    params = ctx.session.createPropertyDetailPayload(listing_id)
                    response = ctx.session.makeRequest('get', url=URLS['listing_details'], params=params)

                    ## Augment the Proerty Detail (t) and Stays API (e) data
                    t = dict_subset(response, 'data', 'presentation', 'stayProductDetailPage')
                    if t and t.get('sections') and not dict_subset(t, 'sections', 'metadata', 'errorData'):
                        e = {**e, **t}
                        ctx.file_mgr.saveJSONFile(e, 'debug', listing_id)
                    else:
                        ## Very rarely, a request returns an error string in the metadata. In this case the listing is ignored
                        logger.warning(f"Listing ID {listing_id} has error metadata. Saved to debug/error_data.json")
                        ctx.file_mgr.saveJSONFile(t, 'debug', 'error_data.json')

                        continue
                else:
                    continue

            ## -------- UNIVERSAL FILTERS & LOGIC --------
            ## Exclude cross-country edge case
            address = dict_subset(e, 'publicAddress')
            if ctx.country == 'Singapore' and address and ctx.country not in address:
                continue

            ## After Listing data, save calendar, description, reviews
            if (self.ctx.isWebPreview and len(self.downloaded_listingIDs) <= 50) or not self.ctx.isWebPreview:
                if ctx.scrapeCalendar:
                    ctx.session.scrapeCalendarToFile(listing_id)

                if ctx.scrapeDescription:
                    ctx.session.scrapeDescriptionToFile(listing_id)
                    if ctx.translateDescriptionToEnglish and ctx.country not in DO_NOT_TRANSLATE:
                        ctx.session.scrapeDescriptionToFile(listing_id, translate=True)

                if ctx.scrapeReviews:
                    ctx.session.scrapeReviewsToFile(e, listing_id)

            ## Finally; Save overview JSON file
            ctx.file_mgr.saveJSONFile(e, 'overview', listing_id)
            
    def _extractListingsFromResponse(self, response):
        """ Find the relevant data subset from the related API call """
        if self.runner_type == 'explore':
            sections = dict_subset(response, 'data', 'dora', 'exploreV3', 'sections')
            return sections[-1]['items'] if sections else []
        elif self.runner_type == 'stays':
            return dict_subset(response, 'data', 'presentation', 'staysSearch', 'results', 'searchResults')
        elif self.runner_type == 'pricing':
            return dict_subset(response, 'data', 'presentation', 'staysSearch', 'results', 'searchResults')
        return []
    
    def _updatePreview_mapTiles(self, coords):
        if coords not in self.preview_mapTileList:
            self.preview_mapTileList.append(coords)
            logger.debug(f"Adding {coords} to preview_mapTile list, list length: {len(self.preview_mapTileList)}")
    
    def getMapTileList(self):
        """
        Paste in an Airbnb URL if not set in config.toml, and extract map tile coordinates.
        For additional instructions on correct URLs, see Readme
        The MapTile list is a list of dicts, starting with one dict.
        """

        ## Load config again for airbnb map search URL
        with open(toml_path, "rb") as f:
            config = tomllib.load(f)
        
        ## Get URL from user, if not set in config.toml
        if config['custom_config']['airbnb_mapsearch_URL'] is None:
            url = input('Paste the URL for the area you want to search:')
        else:
            url = config['custom_config']['airbnb_mapsearch_URL']

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
        
    def iterateMapTiles(self, runner_type, **kwargs):
        """
        Iterates through map tiles to fetch and process listings for a given runner type.

        Method:
            - Sets up the scraper context and initializes the list of map tiles to process.
            - If explore or stays, it starts with the inital map tile. If pricing, it uses the first few map tiles from Explore API.
            - Iterates over each tile's coordinates:
                - Fetches the initial response for the tile.
                - Checks the total number of listings. If the total exceeds 240 and zoom < 23,
                subdivides the tile into smaller tiles to avoid API limits.
                - Processes listings for the tile using `_processTile`.
        Args:
            runner_type (str): Type of data being scraped (explore, stays or pricing).
            **kwargs: Additional keyword arguments for pricing data (check-in date, check-out date etc.)
        """
        self.runner_type = runner_type
        self._setup_scraper_context(**kwargs)

        ## Use MapTile for all
        if not self.ctx.isWebPreview:   ## For non-preview, use full MapTile set
            mapTile_list = self.mapTile_init.copy()
            logger.info(f"Starting {self.runner_type} API map scrape")
        elif self.ctx.isWebPreview and runner_type != 'pricing':  ## For preview, use full MapTile set except for with pricing
            mapTile_list = self.mapTile_init.copy()
            logger.info(f"Starting {self.runner_type} API map scrape")
        else:   # Otherwise, use preview MapTile set for pricing in preview mode
            mapTile_list = self.preview_mapTileList.copy()  
            logger.debug(f"Using preview list, length: {len(mapTile_list)}")
    
        ## Iterate through the list of dicts containing co-ordinates
        while mapTile_list:

            ## Get first co-ordinates and request listings in that area
            coords = mapTile_list.pop(0)
            initial_response = self._fetchInitialTile(coords)
            
            ## Airbnb returns a maximum of 270 listings per area. If there are more listings, divide the co-ordinates
            ## of the above tile, and re-run this process until there are fewer than 270 listings.
            ## Very rarely, the co-ordinates subdivide indefinately, in this case a zoom limit of 23 is set
            total = self.getTotalCount(initial_response)
            
            if total >= 240 and coords['zoom'] < 23:
                logger.debug(f"Over 240 results, dividing map tile. {len(mapTile_list)} map tiles remaining")
                mapTile_list.extend(self.divide_map_tiles(coords))
                continue
            
            ## Extract and save the data
            self._processTile(coords, total, initial_response)
        
        if self.runner_type != 'pricing':
            logger.info(f"Completed {self.runner_type} API scrape")
    
    def getTotalCount(self, response):
        """
        Extract the total number of listings from an Airbnb API Map response.
        Args:
            response (dict): Parsed JSON response from the Airbnb Map API.
        Returns:
            int: Total number of listings found in the response.
        Notes:
            - Extracts integers from strings using regex to handle formatted numbers (e.g., "1,234 listings").
            - Returns 0 if no count can be found or parsed. This causes the map tile iterator to move on to the next
        Raises:
            ValueError: Only raised if runner_type is not valid
        """
        
        if self.runner_type == 'explore':
            totalCount = dict_subset(response, 'data', 'dora', 'exploreV3', 'metadata', 'paginationMetadata', 'totalCount')
            return int(totalCount)
            
        elif self.runner_type in ['stays', 'pricing']:
            totalCount = dict_subset(response, 'data', 'presentation', 'staysSearch', 'results', 'sectionConfiguration', 'pageTitleSections', 'sections', 0, 'sectionData', 'structuredTitle')
            if totalCount is None:
                logger.debug(f"Total Count is {totalCount}. Returning 0")
                return 0
        else:
             raise ValueError(f"Invalid runner_type for getTotalCount: {self.runner_type}")   
        
        ## Extract only integer of total count
        try:
            matches = re.search(r'(\d+,\d+|\d+)', totalCount)  
            totalCount_RegexMatch = matches.group(0).replace(',', '')
        except:
            logger.error("Cannot extract Total Listing Count from map tile. Outputting to debug/cannot_extract_total_listing_count.json")
            file_mgr.saveJSONFile(response, 'debug', 'cannot_extract_total_listing_count.json')

        #logger.debug(f"Found {totalCount_RegexMatch} total listings on this mapTile")
        return int(totalCount_RegexMatch)
    
    def divide_map_tiles(self, coords):
        """
        Subdivide a map tile into four smaller tiles by halving its latitude and longitude bounds.
    
        Args:
            coords (dict): A dictionary containing the map bounds and zoom level, with keys:
                - 'neLat': float – northeast latitude
                - 'neLng': float – northeast longitude
                - 'swLat': float – southwest latitude
                - 'swLng': float – southwest longitude
                - 'zoom': int – current zoom level
    
        Returns:
            list of dict: A list of four coordinate dictionaries representing the subdivided tiles.
                          Each dictionary includes updated 'neLat', 'neLng', 'swLat', 'swLng', and incremented 'zoom'.
    
        Notes:
            - Zoom level is incremented by 1 in all returned tiles
        """
        
        halfLat = (coords['neLat']-coords['swLat'])/2
        halfLng = (coords['neLng']-coords['swLng'])/2
        zoom = coords['zoom']+1
        
        coords_1 = {'neLat': coords['neLat'], 'neLng': coords['neLng'], 'swLat': coords['swLat']+halfLat, 'swLng': coords['swLng']+halfLng, 'zoom': zoom}
        coords_2 = {'neLat': coords['neLat']-halfLat, 'neLng': coords['neLng'], 'swLat': coords['swLat'], 'swLng': coords['swLng']+halfLng, 'zoom': zoom}
        coords_3 = {'neLat': coords['neLat'], 'neLng': coords['neLng']-halfLng, 'swLat': coords['swLat']+halfLat, 'swLng': coords['swLng'], 'zoom': zoom}
        coords_4 = {'neLat': coords['neLat']-halfLat, 'neLng': coords['neLng']-halfLng, 'swLat': coords['swLat'], 'swLng': coords['swLng'], 'zoom': zoom}

        return [coords_1, coords_2, coords_3, coords_4]


def get_next_weekdays(weeks_to_look_forward=50):
    """
    Generates a date ranges for upcoming weekdays and weekends over a specified number of weeks.
    Starting from the next Monday (skipping the current week), this function calculates the Monday–Friday
    (weekday) and Friday–Sunday (weekend) ranges for each week and returns them as a list of date pairs.

    Args:
        weeks_to_look_forward (int, optional): Number of future weeks to include. Defaults to 50.

    Returns:
        List[List[str]]: A list of sublists, each containing:
            - Start date (YYYY-MM-DD)
            - End date (YYYY-MM-DD)
            - Label ('weekday' or 'weekend')

    Example:
        [
            ['2025-07-14', '2025-07-18', 'weekday'],
            ['2025-07-18', '2025-07-20', 'weekend'],
            ...
        ]
    """
    today = datetime.now().date()
    next_year_dates = []
    
    ## Start from the following Monday
    days_until_next_monday = (7 - today.weekday()) % 7
    if today.weekday() == 6: ## If today is Sunday (6), set days until the Monday 8 days ahead
        days_until_next_monday = 8
    elif days_until_next_monday == 0: ## For other days, if today is Monday, set it to 7 (next Monday)
        days_until_next_monday = 7
    
    next_monday = today + timedelta(days=days_until_next_monday)
    
    # Iterate through the future weeks
    for week in range(weeks_to_look_forward):
        monday = next_monday + timedelta(weeks=week)
        friday = monday + timedelta(days=4)
        sunday = monday + timedelta(days=6)
        next_year_dates.append([monday.strftime('%Y-%m-%d'), friday.strftime('%Y-%m-%d'), 'weekday'])
        next_year_dates.append([friday.strftime('%Y-%m-%d'), sunday.strftime('%Y-%m-%d'), 'weekend'])
    
    return next_year_dates

def generate_working_folders(ctx):   
    """ Creates folders overview, pricing, calendar, description, reviews and debug in the data/{location} folder. Each is a repository for listing JSON data """
    output_folder = ctx.output_folder

    ## Create all folders
    directories = [f'{output_folder}', f'{output_folder}/overview', f'{output_folder}/pricing', f'{output_folder}/calendar',f'{output_folder}/description', f'{output_folder}/reviews', f'{output_folder}/debug']
    for directory in directories:
        if os.path.exists(directory):
            logger.info(f'{directory} folder already created')
        else:
            os.makedirs(directory, exist_ok=True)

def runAirbnbScrape(ctx):
    """
    The main Airbnb scraper, which moves through the Airbnb map search function. There are three stages to scraping the data:

    - Explore API:  Gets ~99% of listings with useful overview data. Is an old API and may be retired sooner than other
                    The Explore run also calls calendar, description and reviews APIs for each listing, if requested
    - Stays API:    Gets the remaining ~1% of missed listings, and gets basic pricing data for each listing. This price is
                    automatically generated by Airbnb, and is the price for the first available 5 day period
    - Pricing API:  Iterates through each weekday period (Mon-Fri) and weekend period (Fri-Sun), getting prices for each
                    available listing, when 2,3,4,5 and 6 adults ares staying
    """

    airAPI = WebScraper(ctx)
    airAPI.iterateMapTiles('explore')
    airAPI.iterateMapTiles('stays')

    ## Do not run pricing API if disabled in config.toml
    if ctx.scrapePricing is False:
        return
            
    logger.info("Starting Pricing API map runs")
    ## Iterate through dates
    for date in get_next_weekdays():
        logger.info(f'Pricing run for {date[0]}')
        ## Iterate through number of guests
        for guests in [2,3,4,5,6]:
            airAPI.iterateMapTiles('pricing', guests=guests, start_date=date[0], end_date=date[1], date_label=date[2])   
        
if __name__ == '__main__':
    """
    Main entry point for the Airbnb web scraping and data pipeline.

    This script performs a full end-to-end workflow, including scraping, transformation, 
    cloud integration, and system cleanup.

    Steps:
    1. Setup
       - Initialize context, logging, file manager, session handler, data manager, and GCP manager.
       - Retrieve target location from `config.toml` or prompt the user.
    
    2. Web Scraping
       - Create necessary working folders.
       - Execute Airbnb scraping for the specified location.
       - Save each listing as a JSON file.
    
    3. Data Backup and Transformation
       - Backup JSON files to a compressed tar.gz archive in cloud storage.
       - Convert JSON files into structured CSVs (overview, calendar, pricing, etc.).
       - Push CSVs to BigQuery.
    
    4. Analytics and Reporting
       - Run the Neighbourhood ML module.
       - Trigger Dataform workflows.
       - Generate overview DataFrames from BigQuery and store in cloud.
       - Move preview CSVs to cloud storage and archive them.
       - Update recently updated listings and log completion in BigQuery.
    
    5. Cleanup
       - Remove temporary Airbnb processing and data folders.
    
    6. System Reset
       - Optionally reboot the Linux system to finalize the pipeline.
    """
    
    ## Setup
    logger.info('Starting Airbnb web scrape')

    ctx = Context()
    file_mgr = FileManager(ctx)
    gcp_manager = GCPManager(ctx)
    data_handler = DataHandler(ctx)
    session = SessionHandler(ctx)
    ctx.UpdateContextWithHandlers(file_mgr, gcp_manager, data_handler, session)
    
    logger = setup_logging(ctx.location, ctx.log_level)
    
    ## Web scraper
    generate_working_folders(ctx)
    runAirbnbScrape(ctx)

    ## Backup JSON files in Tar.gz
    file_mgr.BackupFiles_ToTarFile_ToCloud()

    ## Generate CSV from JSON files
    data_handler.CSVfileBuilder_Runner()

    ## Push CSV files to BigQuery
    gcp_manager.CSVtoBigQuery_runner()
    
    ## Run Neighbourhood ML
    Neighbourhood(ctx)

    ## Run Dataform
    gcp_manager.InvokeDataform()
    
    ## Download Output table from BigQuery to Dataframe memory, to cloud storage
    gcp_manager.GenerateOverviewDataFrame()
    gcp_manager.pushOverviewDataFrame_toCloudStorage()

    ## If not web preview mode, stop before additional steps run
    if not ctx.isWebPreview:
        logger.info("No need for preview uploads and website housekeeping, exiting...")
        sys.exit(0)

    ## Move preview files to Cloud Storage
    data_handler.CSVfilePreview_Runner()
    file_mgr.ZipAllPreviewFiles_ToCloud()
    
    ## Update recently updated and aggregated web tables
    #gcp_manager.UpdateAggregatedTables() ## Trial not using
    gcp_manager.UpdateWebsiteTables() 

    ## Log completion
    gcp_manager.LogCompletionInBigQuery()
    
    ## Clean-up
    logger.info("Removing Airbnb chunks folder")
    shutil.rmtree("Airbnb_processing_csv_chunks", ignore_errors=True)
    logger.info("Removing data folder")
    shutil.rmtree(f"data/{ctx.location}", ignore_errors=True)
    logger.info('Completed scrape')

    ## Reset Linux
    logger.info("Starting system reboot...")
    r_sleep(10)
    subprocess.run(['sudo', 'systemctl', 'reboot'], check=True)