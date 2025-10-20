import pandas as pd
import os
import re
import time
from datetime import datetime, timedelta
import base64
import shutil
import csv

## Doorstep Analytics Scripts
from utils import dict_subset, getExchangeRateFromUSD
from schemas import transform_dtypes

## Custom logging script
from config_logging import setup_logging
logger = setup_logging()


class DataHandler():
    """
    Handles multiple JSON files conversion to one CSV file
    """
    
    def __init__(self, context):
        self.ctx = context

        ## Get exchange rate from USD:
        self.exchange_rate = getExchangeRateFromUSD(self.ctx.currency)
        
    def CSVfileBuilder_Runner(self):
        """
        The starting point for converting JSON files to a CSV file
        Iterates through each folder, if data is requested
        """
        ## Get first entry date from the first JSON file's RecordInserted field. Used for Scrape Date field and CSV file naming
        self.first_entry_date = self.getFirstEntryDate()
        self.first_entry_date_str = self.first_entry_date.strftime("%d%m%Y")
        
        ## Generate CSV files
        self.JSONfiles_toCompleteCSVfile('Overview')
        """
        if self.ctx.scrapeCalendar:
            self.JSONfiles_toCompleteCSVfile('Calendar')
        if self.ctx.scrapePricing:
            self.JSONfiles_toCompleteCSVfile('Pricing')
        if self.ctx.scrapeDescription:
            self.JSONfiles_toCompleteCSVfile('Description')
            self.JSONfiles_toCompleteCSVfile('Amenities')
        if self.ctx.scrapeReviews:
            self.JSONfiles_toCompleteCSVfile('Reviews')
        """

    def CSVfilePreview_Runner(self):
        ## Get first entry date from the first JSON file's RecordInserted field. Used for Scrape Date field and CSV file naming
        self.first_entry_date = self.getFirstEntryDate()
        self.first_entry_date_str = self.first_entry_date.strftime("%d%m%Y")

        self.JSONfiles_toPreviewCSVfile('Calendar')
        self.JSONfiles_toPreviewCSVfile('Pricing')
        self.JSONfiles_toPreviewCSVfile('Description')
        self.JSONfiles_toPreviewCSVfile('Amenities')
        self.JSONfiles_toPreviewCSVfile('Reviews')

    def getFirstEntryDate(self):
        """
        Scans all JSON files in the 'overview' output folder, identifies the file with the
        oldest creation time, reads its 'RecordInserted' timestamp, and returns it as a datetime.date.
        """

        # Define the path to the 'overview' folder and list all JSON files in the folder
        folder = f"{self.ctx.output_folder}/overview"
        json_files = [f for f in os.listdir(folder) if f.endswith('.json')]

        # Identify the file with the earliest creation time
        oldest_filename = min(
            json_files,
            key=lambda f: os.path.getctime(os.path.join(folder, f))
        )

        # Read JSON data from the oldest file
        json_data = self.ctx.file_mgr.readJSONFile('overview', oldest_filename)

        # Extract 'RecordInserted' timestamp and convert to date
        date_of_collection = datetime.strptime(
            json_data.get("RecordInserted"),
            "%Y-%m-%d %H:%M:%S"
        ).date()

        return date_of_collection
    
    def JSONfiles_toCompleteCSVfile(self, runner_type, chunk_size=1000):
        """
        Converts JSON listing files for a specific runner type into a single CSV file, processing in chunks.

        This method:
            - Scans the output folder for JSON files of the given runner type.
            - Applies a runner-specific transformation method to each JSON file, converting it to a DataFrame.
            - Saves intermediate CSV chunk files for every `chunk_size` records to avoid memory overload.
            - Cleans line breaks in string fields to prevent CSV formatting issues.
            - Combines all CSV chunks into a single output CSV file without fully loading into memory.
            - Optionally opens the CSV file after completion.

        Args:
            runner_type (str): The type of data being processed (e.g., 'stays', 'explore', 'pricing').
            chunk_size (int, optional): Number of records per CSV chunk. Defaults to 1000.

        Returns:
            str: The path to the final combined CSV file.
        """

        logger.info(f'Generating CSV chunks for {runner_type}; chunk size {chunk_size}')
        
        ## Clean folder names and file lists
        if runner_type == 'Amenities':
            folder = 'description'
        else:
            folder = runner_type.lower()

        file_dir = f'{self.ctx.output_folder}/{folder}'
        file_list = os.listdir(file_dir)
        df_list = []
        
        chunk_working_dir = "Airbnb_processing_csv_chunks"          ## Directory to save csv chunk files
        shutil.rmtree(chunk_working_dir, ignore_errors=True)        ## Deletes chunk folder if it exists
        os.makedirs(chunk_working_dir, exist_ok=True)               ## Recreates the folder
        
        chunk_counter = 1 
        
        ## Iterate through each chunk
        for idx, filename in enumerate(sorted(file_list)):
            json_data = self.ctx.file_mgr.readJSONFile(folder, filename)
                
            file_path = os.path.join(file_dir, filename)
            if os.path.getsize(file_path) < 1024:   ## Skip very small JSON files, likely Airbnb error was returned
                logger.debug(f"JSON file {file_dir} is less than 1024 bytes")
                continue
        
            ## Apply the correct transformation according to the runner_type, then append the output to a dataframe list
            method = self.getMethod(runner_type)
            df = method(json_data)
            df_list.append(df)
            
            ## Save chunk if chunk size is reached or it's the last file
            if (idx + 1) % chunk_size == 0 or idx + 1 == len(file_list):
                chunk_df = pd.concat(df_list, ignore_index=True)
                
                chunk_filename = f"chunk_{chunk_counter}.csv"
                chunk_path = os.path.join(chunk_working_dir, chunk_filename)

                ## Escape \r and \n characters to avoid lines bleeding in CSV format
                chunk_df = chunk_df.map(
                    lambda x: re.sub(r"[\r\n]", " ", x) if isinstance(x, str) else x
                )

                ## Use Quote Minimal and na_rep to avoid additional text and paragraph bleeding
                chunk_df.to_csv(chunk_path, index=False, na_rep="", encoding="utf-8", quoting=csv.QUOTE_MINIMAL)
                logger.info(f"Saved chunk_{chunk_counter}.csv to {chunk_working_dir}")
        
                ## Clear the dataframe list for the next chunk
                df_list = []
                chunk_counter += 1
                
        ## Combine CSV chunks into one dataframe without loading into Pandas, to not overload memory
        csv_files = sorted([os.path.join(chunk_working_dir, f) for f in os.listdir(chunk_working_dir) if f.endswith('.csv')])
        
        csv_output_filename = f'{self.ctx.output_folder}/Airbnb_{self.ctx.location}_{self.ctx.country}_{runner_type}_{self.first_entry_date_str}.csv'
        self.ctx.file_mgr.CombineCSVchunksToOneFile(csv_output_filename, csv_files)

        if self.ctx.openCSVonCompletion:
            time.sleep(4)   ## Ensure the file is ready before being opened
            self.ctx.file_mgr.openCSVWithDefault(csv_output_filename)
        
        return csv_output_filename
    
    def JSONfiles_toPreviewCSVfile(self, runner_type):   
        logger.info(f'Generating Preview CSV for {runner_type}')
        
        ## Amenities data is taken from description data
        if runner_type == 'Amenities':
            folder = 'description'
        else:
            folder = runner_type.lower()
            
        file_dir = f'{self.ctx.output_folder}/{folder}'
        
        ## Make sure preview listing IDs are consistent, use IDs seen in Description folder only
        source_files = set(os.listdir(f'{self.ctx.output_folder}/description'))
        matching_files = source_files.intersection(set(os.listdir(file_dir)))

        df_list = []
        
        ## Iterate through each JSON file, apply transformations to Data
        for filename in matching_files:
            json_data = self.ctx.file_mgr.readJSONFile(folder, filename)
                
            ## If file is saved with less than 1kb, is error, skip the file
            file_path = os.path.join(file_dir, filename)
            if os.path.getsize(file_path) < 1024:
                logger.debug(f'File {filename} is blank, skipping')
                continue
        
            method = self.getMethod(runner_type)
            df = method(json_data)
            df_list.append(df)
            
        df_initial = pd.concat(df_list, ignore_index=True)
        
        ## Add additional rows to CSV Preview files
        blank_row = pd.DataFrame([[None] * len(df.columns)], columns=df.columns)
        if runner_type == 'Review':
            extra_text = ' and 20 reviews per listing'
        else:
            extra_text = ''
        message_row = pd.DataFrame([[f"Doorstep Analytics preview; data limited to 50 listings{extra_text}. For full datasets contact info@doorstepanalytics.com"] + [None] * (len(df.columns) - 1)], columns=df.columns)
        
        ## Generate CSV files
        output_file_name = f'{self.ctx.output_folder}/DoorstepAnalyticsPreview_{self.ctx.location}_Airbnb_{runner_type}.csv'
        df = pd.concat([df_initial, blank_row, message_row], ignore_index=True)
        df.to_csv(output_file_name, index=False, encoding="utf-8")
        
        ## Push to GCP Preview Bucket
        self.ctx.gcp_manager.PushCSVtoCloud(output_file_name, 'preview')
    
    def getMethod(self, runner_type):
        """
        For each runner_type, load the corresponding function to parse JSON data to a Pandas dataframe
        """

        airbnb_methods = {
            'Overview': self.transform_AirbnbOverview,
            'Calendar': self.transform_AirbnbCalendar,
            'Pricing': self.transform_AirbnbPricing,
            'Description': self.transform_AirbnbDescription,
            'Reviews': self.transform_AirbnbReviews,
            'Amenities': self.transform_AirbnbAmenities
        }
    
        return airbnb_methods.get(runner_type)
    
    def transform_AirbnbOverview(self, data):
        """
        Transforms listing data from the Overview folder into a pandas DataFrame

        Handles data from different Airbnb APIs:
            - Explore API: provides the most complete data (~99% of cases).
            - Stays API, if above not available: data not fully complete and requires regex extraction from titles or sections.

        Extracted fields include:
            - Listing metadata: ID, title, location, city, neighborhood, coordinates.
            - Host details: ID, first name, languages, superhost status.
            - Listing details: bedrooms, bathrooms, beds, capacity, room type, space type.
            - Review and rating information: review count, average rating, star rating.
            - Pricing details (if provided): basic nightly price, Airbnb service fee, cleaning fee, taxes.

        Args:
            data (dict): Raw Airbnb listing data from JSON file.

        Returns:
            pd.DataFrame: A single-row DataFrame containing transformed listing data.

        Note: Cleaning Fee and Airbnb Service Fee were removed from the search results in Oct. 2025
        """
        
        ## If the Explore API ran successfully (in ~99% of cases), data is extracted here
        if len(data) > 20 and 'id' in data:
            this_row_dict = {
                'Country': self.ctx.country,
                'Location': self.ctx.location,
                'Airbnb_ListingID': data['id'],
                'Scrape_Date': self.first_entry_date,
                'Lat': data['lat'],
                'Lng': data['lng'],
                'City': data['city'],
                'LocalizedCity': data['localizedCity'],
                'LocalizedNeighborhood': data['localizedNeighborhood'],
                'ListingTitle': data['name'],
                'Bathrooms': data['bathrooms'],
                'Bedrooms': data['bedrooms'],
                'Beds': data['beds'],
                'PersonCapacity':data['personCapacity'],
                'is_NewListing': data['isNewListing'],
                'is_Superhost': data['isSuperhost'],
                'PictureCount': data['pictureCount'],
                'isLimitedAirbnbData': False,
                'Host_Languages': str(data['hostLanguages']),
                'Host_ID': data['user']['id'], 
                'Host_FirstName': data['user']['firstName'],
                'Host_isSuperhost': data['user']['isSuperhost'],
                'RoomType': data['roomType'],
                'SpaceType': data['spaceType'],
                'RoomAndPropertyType': data['roomAndPropertyType'],
                'ReviewCount': data['reviewsCount'],
                'AvgRating': data['avgRating'],
                'StarRating': data['starRating'],
                'AmenityIDs': str(data['amenityIds'])
            }
        
        ## If Explore API did not capture the listing, data is extracted here
        ## This data is a little less complete than above
        elif 'sections' in data:
            demand_listing = data['demandStayListing']
            section = data['sections']
            
            ## Extract data from title
            title_str = dict_subset(section, 'metadata', 'sharingConfig', 'title')
            bathrooms = textExtractRegex(title_str, r'(\d+(\.\d+)?)\s*\w*\s*bathroom')
            bedrooms = textExtractRegex(title_str, r'(\d+(\.\d+)?)\s*\w*\s*bedroom')
            beds = textExtractRegex(title_str, r'(\d+(\.\d+)?)\s*bed')

            generated_title = data['title']
            localized_neighborhood = textExtractRegex(generated_title, r'\bin\s+(.*)')

            ## Identify room type data
            room_type = dict_subset(section, 'metadata', 'loggingContext', 'eventDataLogging', 'roomType')
            room_type = "Entire home/flat" if room_type == "Entire home/apt" else room_type
            avg_rating = None if dict_subset(section, 'metadata', 'sharingConfig', 'starRating') == 0 else dict_subset(section, 'metadata', 'sharingConfig', 'starRating')
            
            ## Identify host data
            for p in section['sbuiData']['sectionConfiguration']['root']['sections']:
                if dict_subset(p, 'sectionId') == 'HOST_OVERVIEW_DEFAULT':
                    host_id = dict_subset(p, 'loggingData', 'eventData', 'pdpContext', 'hostId')
                    host_issuperhost = True if dict_subset(p, 'loggingData', 'eventData', 'pdpContext', 'isSuperHost') == 'true' else False
                    host_firstname = str(textExtractRegex(dict_subset(p, 'sectionData', 'title'), r'(?:Hosted by|Stay with) (\w+)'))

            this_row_dict = {
                'Country': self.ctx.country,
                'Location': self.ctx.location,
                'Airbnb_ListingID': dict_subset(section, 'metadata', 'loggingContext', 'eventDataLogging', 'listingId'),
                'Scrape_Date': self.first_entry_date,
                'Lat': dict_subset(demand_listing, 'location', 'coordinate', 'latitude'),
                'Lng': dict_subset(demand_listing, 'location', 'coordinate', 'longitude'),
                'City': dict_subset(section, 'metadata', 'sharingConfig', 'location'),
                'LocalizedCity': dict_subset(section, 'metadata', 'seoFeatures', 'neighborhoodBreadcrumbDetails', 0, 'linkText'),
                'LocalizedNeighborhood': localized_neighborhood,
                'ListingTitle': dict_subset(data, 'nameLocalized', 'localizedStringWithTranslationPreference'),
                'Bathrooms': bathrooms,
                'Bedrooms': bedrooms,
                'Beds': beds,
                'PersonCapacity': dict_subset(section, 'metadata', 'sharingConfig','personCapacity'),
                'is_NewListing': None,
                'is_Superhost': dict_subset(section, 'metadata', 'loggingContext', 'eventDataLogging', 'isSuperhost'),
                'PictureCount': dict_subset(section, 'metadata', 'loggingContext', 'eventDataLogging', 'pictureCount'),
                'isLimitedAirbnbData': True,    ## True for Stays API only data, false is Explore API used
                'Host_Languages': None,
                'Host_ID': host_id,
                'Host_FirstName': host_firstname,
                'Host_isSuperhost': host_issuperhost,
                'RoomType': room_type,
                'SpaceType': dict_subset(section, 'metadata', 'sharingConfig', 'propertyType'),
                'RoomAndPropertyType': dict_subset(section, 'metadata', 'sharingConfig', 'propertyType'),
                'ReviewCount': dict_subset(section, 'metadata', 'sharingConfig', 'reviewCount'),
                'AvgRating': avg_rating,
                'StarRating': dict_subset(section, 'metadata', 'sharingConfig', 'starRating'),
                'AmenityIDs': None
            }

        ## Pricing from Stays API, if provided by Airbnb
        airbnb_service_fee, cleaning_fee, taxes, basic_night_price_string, basic_night_price = None, None, None, None, None
        pricing_quote = dict_subset(data, 'structuredDisplayPrice', 'explanationData', 'priceDetails')
        if pricing_quote:
            for price in pricing_quote[0]['items']:
                if price['description'] == 'Airbnb service fee':
                    ## Removed by Airbnb, Oct 2025
                    airbnb_service_fee = extractPricingValue(price['priceString'])
                elif price['description'] == 'Cleaning fee':
                    ## Removed by Airbnb, Oct 2025
                    cleaning_fee = extractPricingValue(price['priceString'])
                elif 'Taxes' in price['description']:
                    taxes = extractPricingValue(price['priceString'])
                elif price['description'] == "Resort fee":
                    ## Not Used
                    resort_fee = extractPricingValue(price['priceString'])
                elif price['description'] == "Management fee":
                    ## Not Used
                    management_fee = extractPricingValue(price['priceString'])
                elif price['displayComponentType'] == 'DEFAULT_EXPLANATION_LINE_ITEM':
                    if " x " in price['description']:
                        basic_night_price_string = price['description']
                        basic_night_price = extractPricingValue(str(price['description']).split(" x ")[1])
                 
        ## Update with pricing data
        this_row_dict.update({
            'BasicNightPrice_CheckIn': dict_subset(data, 'listingParamOverrides', 'checkin'),
            'BasicNightPrice_CheckOut': dict_subset(data, 'listingParamOverrides', 'checkout'),
            'Currency': self.ctx.currency,
            'BasicNightPrice': None if basic_night_price is None else round(self.exchange_rate * basic_night_price, 2),
            #'CleaningFee': None if cleaning_fee is None else round(self.exchange_rate * cleaning_fee, 2),
            #'AirbnbServiceFee': None if airbnb_service_fee is None else round(self.exchange_rate * airbnb_service_fee, 2),
            'Taxes': None if taxes is None else round(self.exchange_rate * taxes, 2),
            'BasicNightPriceString_USD': basic_night_price_string,
            'BasicNightPrice_USD': basic_night_price,
            #'CleaningFee_USD': cleaning_fee,
            #'AirbnbServiceFee_USD': airbnb_service_fee,
            'Taxes_USD': taxes,
            'RecordInserted': datetime.strptime(data['RecordInserted'], '%Y-%m-%d %H:%M:%S')
        })
        
        return pd.DataFrame([this_row_dict])
    
    def transform_AirbnbCalendar(self, data):
        """
        Transforms listing data from the Calendar folder into a pandas DataFrame

        Handles data from different Airbnb APIs:
            - Explore API: provides the most complete data (~99% of cases).
            - Stays API, if above not available: data not fully complete and requires regex extraction from titles or sections.

        Each row represents a single listing on a specific date, with additional fields:
            - Days from the initial scrape date
            - Month and year of the calendar date
            - Weekday vs weekend classification
            - Availability and booking status
            - Minimum and maximum nights allowed
            - Check-in/check-out availability

        Args:
            data (dict): Raw calendar data for a single Airbnb listing, with 'Calendar' as a list of date entries with availability info

        Returns:
            pd.DataFrame: DataFrame containing transformed calendar data for one listing
        """

        ## Iterate through each row of the Calendar JSON file
        rows = []
        for i in data['Calendar']:
            this_row_dict = {}
            
            ## Each row is a date for each listing, from the scrape data for the following year
            ## Apply some calculations to categorize date data
            calendar_date = datetime.strptime(i['Calendar_Date'], '%Y-%m-%d').date()
            months_difference = (calendar_date.year - self.first_entry_date.year) * 12 + (calendar_date.month - self.first_entry_date.month)
            if calendar_date.weekday() in [4, 5]:
                stay_week = 'Weekend'
            else:
                stay_week = 'Weekday'
                
            this_row_dict = {
                'Country': self.ctx.country,
                'Location': self.ctx.location,
                'Airbnb_ListingID': data['ListingID'],
                'Scrape_Date': self.first_entry_date,
                'Calendar_Date': calendar_date,
                'DaysFrom_ScrapeDate': (calendar_date - self.first_entry_date).days,
                'Calendar_Month': calendar_date.month,
                'MonthsFrom_ScrapeDate': months_difference,
                'Calendar_Year': calendar_date.year,
                'DayOfWeek': self.first_entry_date.strftime('%A'),
                'DayOfWeek_Int': self.first_entry_date.isoweekday(),
                'Weekday_Or_Weekend': stay_week,
                'isAvailable': i['isAvailable'],
                'isBooked': not i['isAvailable'],
                'Min_Nights': i['Min_Nights'],
                'Max_Nights': i['Max_Nights'],
                'Available_For_Checkin': i['Available_For_Checkin'],
                'Available_For_Checkout': i['Available_For_Checkout'],
                'RecordInserted': datetime.strptime(data['RecordInserted'], "%Y-%m-%d %H:%M:%S"),
            }
            
            rows.append(this_row_dict)
        
        df = pd.DataFrame(rows)
        return df

    def transform_AirbnbPricing(self, data):
        """
        Transforms Airbnb pricing JSON data into a structured DataFrame.

        Each row represents a pricing period for a single listing, either over weekdays (Mon-Fri) or weekends (Fri-Sun)

        The function extracts:
            - Check-in and check-out dates
            - Number of guests
            - Pricing components (base price, cleaning fee, taxes, Airbnb service fee, discounts)

        Args:
            data (dict): Raw pricing data for a single Airbnb listing from Pricing JSON file

        Returns:
            pd.DataFrame: DataFrame containing transformed pricing data for one listing

        Note: Cleaning Fee and Airbnb Service Fee were removed from the search results in Oct. 2025
        """
        
        ## Iterate through each row of the Pricing JSON file
        rows = []
        for i in data['prices']:
            this_row_dict = {}
            
            ## Ignore if there is no display price saved
            if dict_subset(i, 'structuredDisplayPrice') is None:
                logger.warning(f"Pricing file has no pricing data for {data['listingid']}")
                continue
    
            ## Convert data strings to date objects
            pricing_start_date = datetime.strptime(i['start_date'], '%Y-%m-%d')
            pricing_end_date = datetime.strptime(i['end_date'], '%Y-%m-%d')
    
            ## Create empty dict for fees, to append data to
            fees = dict.fromkeys([
                "cleaning_fee",
                "taxes",
                "airbnb_service_fee",
                "discount_amount",
                "price_int",
                "discount_description",
                "price_verbose"
            ], None)
            
            ## Iterate through price items, where each item is a row explaining a price element, such as cleaning fee or taxes
            price_data = dict_subset(i, 'structuredDisplayPrice', 'explanationData')
            if price_data:
                items = price_data.get('priceDetails', [{}])[0].get('items', [])
            
                for price in items:
                    desc = price.get('description')
                    price_str = price.get('priceString')
            
                    if price.get('displayComponentType') == 'DISCOUNTED_EXPLANATION_LINE_ITEM':
                        if ' x ' not in desc:
                            fees["discount_description"] = desc
                            fees["discount_amount"] = extractPricingValue(price_str)
                    elif desc == "Cleaning fee":
                        ## Removed by Airbnb, Oct 2025
                        fees["cleaning_fee"] = extractPricingValue(price_str)
                    elif desc == "Airbnb service fee":
                        ## Removed by Airbnb, Oct 2025
                        fees["airbnb_service_fee"] = extractPricingValue(price_str)
                    elif desc == "Resort fee":
                        ## Not Used
                        resort_fee = extractPricingValue(price_str)
                    elif desc == "Management fee":
                        ## Not Used
                        management_fee = extractPricingValue(price['priceString'])
                    elif desc == "Taxes":
                        fees["taxes"] = extractPricingValue(price_str)
                    
                    if ' x ' in desc:      ## The standard price per night, in format "5 x £50 nights"
                        fees["price_verbose"] = desc
                        price_split = str(desc).split(" x ")[1]
                        fees["price_int"] = extractPricingValue(price_split)
    
                this_row_dict = {
                    'Country': self.ctx.country,
                    'Location': self.ctx.location,
                    'Airbnb_ListingID': data['listingid'],
                    'Scrape_Date': self.first_entry_date,
                    'Guests': i.get('adults'),
                    'Month': pricing_start_date.month,
                    'Week_label': i.get('week_label'),
                    'Stay_Checkin': pricing_start_date,
                    'Stay_Checkout': pricing_end_date,
                    'Stay_Length_Actual': abs((pricing_end_date - pricing_start_date).days),
                    'Currency': data.get('currency'),
                    'PricePerNight': None if fees["price_int"] is None else round(self.exchange_rate * fees["price_int"], 2),
                    'DiscountDescription': fees['discount_description'],
                    'DiscountAmount': None if fees["discount_amount"] is None else round(self.exchange_rate * fees["discount_amount"], 2),
                    #'CleaningFee': None if fees["cleaning_fee"] is None else round(self.exchange_rate * fees["cleaning_fee"], 2),
                    #'AirbnbServiceFee': None if fees["airbnb_service_fee"] is None else round(self.exchange_rate * fees["airbnb_service_fee"], 2),
                    'Taxes': None if fees["taxes"] is None else round(self.exchange_rate * fees["taxes"], 2),
                    'PriceVerbose_USD': fees["price_verbose"],
                    'PricePerNight_USD': fees["price_int"],
                    'DiscountAmount_USD': fees['discount_amount'],
                    #'CleaningFee_USD': fees['cleaning_fee'],
                    #'AirbnbServiceFee_USD': fees['airbnb_service_fee'],
                    'Taxes_USD': fees['taxes'],
                    'RecordInserted': data.get('RecordInserted')
                }
            
            else:   ## If explanationData is blank, no pricing data provided
                logger.warning(f"Pricing file has no pricing data for {data['listingid']}")
                continue
    
            rows.append(this_row_dict)
    
        df = pd.DataFrame(rows)
        return df

    def transform_AirbnbDescription(self, json_data):
            """
            Transforms Description JSON data for one listing into a structured pandas DataFrame.

            This function extracts detailed listing descriptions, host information, amenities, 
            house rules, check-in/check-out times, photo captions, and ratings. It handles both 
            original and translated descriptions, prioritizing translated content if available.

            Args:
                json_data (dict): Raw JSON data containing the listing description, including keys:
                - 'originalDescription': Original listing text as writen in host's language
                - 'translatedDescription': Translated text to English (if selected)

            Returns:
                pd.DataFrame: Single-row DataFrame with data for that listing
            """

            df = pd.DataFrame({col: pd.Series(dtype=dt) for col, dt in transform_dtypes['description_dtypes'].items()})
            this_row_dict = {}
            
            ## Identify if only Original Descriptions are requested, or if English translations are included in the JSON file
            original = json_data.get('originalDescription')
            translated = json_data.get('translatedDescription')
            
            ## If no description data found in file, log and debug
            if original is None and translated is None:
                logger.warning("No Description data found in file. Outputting to debug/no_description_data.json")
                self.ctx.file_mgr.saveJSONFile(main_description, 'debug', 'no_description_data')
                return None
            
            ## Main Description will contain basic values. This is normally the original description values, in case of failure, use the translated description
            if translated is not None and dict_subset(json_data, 'translatedDescription', 'metadata', 'errorData') is None:
                try:
                    if len(dict_subset(translated, 'metadata', 'loggingContext', 'eventDataLogging')) > len(dict_subset(original, 'metadata', 'loggingContext', 'eventDataLogging')):
                        main_description = translated
                    else:
                        main_description = original
                except (KeyError, TypeError):
                    main_description = original
            else:
                main_description = original

            ## eventData is required for basic listing details. Can be missing on rare 500 errors where there is an error on Airbnb's API
            eventData = dict_subset(main_description, 'metadata', 'loggingContext', 'eventDataLogging')
            if eventData is None:
                logger.warning("No Description event data found in file. Outputting to debug/no_description_event_data.json")
                self.ctx.file_mgr.saveJSONFile(main_description, 'debug', 'no_description_event_data')
                return None
            
            ## Inital entries into Description row
            this_row_dict.update({
                'Country': self.ctx.country,
                'Location': self.ctx.location,
                'Airbnb_ListingID': eventData['listingId'],
                'Scrape_Date': self.first_entry_date,
                'ReviewCount': dict_subset(eventData, 'visibleReviewCount') if dict_subset(eventData, 'visibleReviewCount') is not None else 0,
                'AccuracyRating': dict_subset(eventData, 'accuracyRating'),
                'CheckInRating': dict_subset(eventData, 'checkinRating'),
                'CleanlinessRating': dict_subset(eventData, 'cleanlinessRating'),
                'CommunicationRating': dict_subset(eventData, 'communicationRating'),
                'LocationRating': dict_subset(eventData, 'locationRating'),
                'ValueRating': dict_subset(eventData, 'valueRating'),
                'GuestSatisfactionOverall': dict_subset(eventData, 'guestSatisfactionOverall'),
                'PictureCount': eventData['pictureCount'],
                'CancellationPolicy': dict_subset(main_description, 'metadata', 'bookingPrefetchData', 'cancellationPolicies', 0, 'localized_cancellation_policy_name')
            })
            
            ## Set empty lists to be filled
            additional_house_rules = []
            room_arrangement_title = []
            room_arrangement_subtitle = []
            host_highlights = []
            amenities_list = []
            caption_list = []
            caption_list_translated = []
            
            ## Iterate through Translated and Original text descriptions
            ## Save translated as fields with _Localized suffix
            for d in ['original', 'translated']:                
                if d == 'translated':
                    if translated is None:
                        break
                    description = translated
                    field_suffix = '_Localized'
                elif d == 'original':
                    description = original
                    field_suffix = ''

                ## Property text descriptions
                for section in description['sections']:
                    if section['sectionId'] == 'DESCRIPTION_MODAL':
                        for sub_section in section['section']['items']:
                            if sub_section.get('title') == 'The space':
                                this_row_dict[f'SpaceDescription{field_suffix}'] = dict_subset(sub_section, 'html', 'htmlText')
                            elif sub_section.get('title') == 'Guest access':
                                this_row_dict[f'GuestAccessDescription{field_suffix}'] = dict_subset(sub_section, 'html', 'htmlText')
                            elif sub_section.get('title') == 'During your stay':
                                this_row_dict[f'DuringStayDescription{field_suffix}'] = dict_subset(sub_section, 'html', 'htmlText')
                            elif sub_section.get('title') == 'Registration number' and description == 'original':
                                this_row_dict['RegistrationNumber'] = dict_subset(sub_section, 'html', 'htmlText')
                            else:
                                this_row_dict[f'MainDescription{field_suffix}'] = dict_subset(sub_section, 'html', 'htmlText')

                    if section.get('sectionId') == 'TITLE_DEFAULT':
                        this_row_dict[f'ListingTitle{field_suffix}'] = section['section']['title'] 
                        
                    ## Location description text
                    if section.get('sectionId') == 'LOCATION_DEFAULT':
                        this_row_dict[f'LocationDescription{field_suffix}'] = dict_subset(section, 'section', 'seeAllLocationDetails', 0, 'content', 'htmlText')

                    ## Host About text
                    if section.get('sectionId') in ['HOST_OVERVIEW_DEFAULT', 'MEET_YOUR_HOST']:
                        if dict_subset(section, 'section', 'about') is not None and dict_subset(section, 'section', 'about') != '':
                            this_row_dict[f'HostAbout{field_suffix}'] = dict_subset(section, 'section', 'about')
                        
                    ## Photo captions (often not included)
                    if section.get('sectionId') == 'PHOTO_TOUR_SCROLLABLE_MODAL' and d == 'translated':
                        for img in section['section']['mediaItems']:
                            if img['imageMetadata'].get('caption'):
                                caption_list.append(img['imageMetadata']['caption'])
                                if img['imageMetadata'].get('localizedCaption'):
                                    caption_list_translated.append(img['imageMetadata']['localizedCaption'])
                            elif not img['accessibilityLabel'].startswith('Listing image '):
                                caption_list.append(img['accessibilityLabel'])

            ## No translation of the following
            for section in main_description['sections']:

                ## Room Details section
                if section.get('sectionId') == 'SLEEPING_ARRANGEMENT_DEFAULT':
                    for arrangement in section['section']['arrangementDetails']:
                        room_arrangement_title.append(arrangement.get('title'))
                        room_arrangement_subtitle.append(arrangement.get('subtitle'))

                ## Host Details section  
                if section.get('sectionId') == 'MEET_YOUR_HOST':
                    this_row_dict['HostName'] = dict_subset(section, 'section', 'cardData', 'name') 
                    this_row_dict['Host_RatingCount'] = dict_subset(section, 'section', 'cardData', 'ratingCount') 
                    this_row_dict['Host_RatingAverage'] = dict_subset(section, 'section', 'cardData', 'ratingAverage') 
                    this_row_dict['Host_TimeMonths'] = dict_subset(section, 'section', 'cardData', 'timeAsHost', 'months') 
                    this_row_dict['Host_TimeYears'] = dict_subset(section, 'section', 'cardData', 'timeAsHost', 'years')
                    this_row_dict['Host_isSuperhost'] = dict_subset(section, 'section', 'cardData', 'isSuperhost')
                    this_row_dict['Host_isVerified'] = dict_subset(section, 'section', 'cardData', 'isVerified')
                    
                    if dict_subset(section, 'section', 'businessDetailsItem', 'title') == 'This listing is offered by an individual. Learn more':
                        this_row_dict['Host_BusinessType'] = 'Individual'
                    else:
                        this_row_dict['Host_BusinessType'] = dict_subset(section, 'section', 'businessDetailsItem', 'title')
                    
                    ## Host ID is an integer, decode from Base64
                    if dict_subset(section, 'section', 'cardData', 'userId'):
                        this_row_dict['Host_ID'] = base64.b64decode(dict_subset(section, 'section', 'cardData', 'userId')).decode("utf-8")[11:]
                      
                    if dict_subset(section, 'section', 'hostDetails'):
                        for detail in dict_subset(section, 'section', 'hostDetails'):
                            if detail.startswith('Response rate: '):
                                this_row_dict['Host_ResponseRate'] = detail[len('Response rate: '):]
                            elif detail.startswith('Responds'):
                                this_row_dict['Host_ResponseTime'] = detail
                    
                    if dict_subset(section, 'section', 'hostHighlights'):
                        for highlight in dict_subset(section, 'section', 'hostHighlights'):
                            host_highlights.append(highlight['title'])
            
                ## Check in times and house rules
                if section.get('sectionId') == 'POLICIES_DEFAULT':
                    for rule in section['section']['houseRules']:
                       if rule['title'].startswith('Check-in: '):
                           this_row_dict['CheckIn_Start'] = rule['title'][10:15]
                           this_row_dict['CheckIn_End'] = rule['title'][18:23]
                       elif rule['title'].startswith('Check-in after'):
                           this_row_dict['CheckIn_Start'] = rule['title'][15:20]
                       elif rule['title'].startswith('Checkout before '):
                           this_row_dict['CheckOut_End'] = rule['title'][16:21]
                    if len(section['section']['houseRulesSections']) > 1:
                        for rule in section['section']['houseRulesSections'][1]['items']:
                            if rule['title'] == 'Additional rules':
                                additional_house_rules.append(f"{rule['title']}: {rule['html']['htmlText']}")
                            else:
                                additional_house_rules.append(f"{rule['title']}: {rule['subtitle']}" if rule.get('subtitle') else rule['title'])    
                
                ## Amenities, converted from a JSON list to a list within an str()
                if section.get('sectionId') == 'AMENITIES_DEFAULT':
                    for item in section['section']['seeAllAmenitiesGroups']:
                        for amty in item['amenities']:
                            if amty['available']:
                                amenities_list.append(f"{amty['title']}: {amty['subtitle']}" if amty.get('subtitle') else amty['title'])    
            
            this_row_dict.update({
                'Host_Highlights': str(host_highlights),
                'RoomTitles': str(room_arrangement_title),
                'RoomSubTitles': str(room_arrangement_subtitle),
                'AdditionalHouseRules': str(additional_house_rules),
                'PhotoCaptions': str(caption_list),
                'PhotoCaptions_Localized': str(caption_list_translated),
                'Amenities': str(amenities_list),
                'RecordInserted': json_data['RecordInserted']
            })
            
            ## Try to stop text bleed
            for key, value in this_row_dict.items():
                if isinstance(value, str):  ## Apply only to string values
                    this_row_dict[key] = value.replace('\n', '\\n').replace('\r', '\\r')
                    
            df = pd.concat([df, pd.DataFrame([this_row_dict])], ignore_index=True)
            return df
        
    def transform_AirbnbReviews(self, json_data):
        """
        Transforms listing Reviews JSON data into a structured pandas DataFrame.

        This function processes reviews for a given listing, extracting information such as review text, reviewer details,
        host details and responses, ratings, type of trip, length of stay etc.

        Args:
            json_data (dict): Raw JSON data which iterates through 'reviews', a list of review objects

        Returns:
            pd.DataFrame: DataFrame where each row represents a single review for the listing
        """
        
        ## Iterate through each row of the Reviews JSON file
        rows = []
        for review in json_data['reviews']:
            this_row_dict = {}
            
            ## Translated reviews and review responses to English
            comments_localized = dict_subset(review, 'localizedReview', 'comments')
            response_localized = dict_subset(review, 'localizedReview', 'response')
            
            ## Extract optional information, not always provided by Airbnb
            if review.get('highlightType') == 'TYPE_OF_TRIP':
                type_of_trip = dict_subset(review, 'reviewHighlight')
            else:
                type_of_trip = None
                
            if review.get('highlightType') == 'LENGTH_OF_STAY':
                length_of_stay = dict_subset(review, 'reviewHighlight')
            else:
                length_of_stay = None
            
            ## Set default values, to be modified below if True / contains values
            reviewer_new_to_airbnb = False
            reviewer_years, reviewer_months, reviewer_region, reviewer_country = None, None, None, None

            ## Review information contains either monhts / years the guest is on Airbnb, or their location
            if review['localizedReviewerLocation'] is not None:
                if ',' not in review['localizedReviewerLocation']:
                    match_years = re.search(r'(\d+)\s+years?\s+on\s+Airbnb', dict_subset(review, 'localizedReviewerLocation'))
                    match_month = re.search(r'(\d+)\s+months?\s+on\s+Airbnb', dict_subset(review, 'localizedReviewerLocation'))
                    if match_years:    
                        reviewer_years = int(match_years.group(1))
                    elif match_month:    
                        reviewer_months = int(match_month.group(1))
                    else:
                        reviewer_country = dict_subset(review, 'localizedReviewerLocation')
                        if reviewer_country == 'New to Airbnb':
                            reviewer_country = None
                            reviewer_new_to_airbnb = True
                else:
                    reviewer_region, reviewer_country = dict_subset(review, 'localizedReviewerLocation').split(', ', 2)[:2]
                    
            ## Get data with defaults to None or 0
            reviewer_pictureURL = None if (url := dict_subset(review, 'reviewer', 'pictureUrl')) and 'Portrait/Avatars' in url else url
            host_pictureURL = None if (url := dict_subset(review, 'reviewee', 'pictureUrl')) and 'Portrait/Avatars' in url else url
            isHostHighlightedReview = False if dict_subset(review, 'isHostHighlightedReview') is None else True
            photocounts = len(review['reviewPhotoUrls']) if review['reviewPhotoUrls'] is not None else 0
            
            this_row_dict = {
                'Country': self.ctx.country,
                'Location': self.ctx.location,
                'Airbnb_ListingID': json_data['ListingID'],
                'Scrape_Date': self.first_entry_date,
                'ReviewID': review['id'],
                'Review_CreatedAt': review['createdAt'],
                'ReviewLanguage': review['language'],
                'ReviewComments': review['comments'],
                'ReviewComments_Localized': comments_localized,
                'Review_PhotoCounts': photocounts,
                'Rating': dict_subset(review, 'rating'),
                'TypeOfTrip': type_of_trip,
                'LengthOfStay': length_of_stay,
                'Reviewer_ID': review['reviewer']['id'],
                'Reviewer_Deleted': review['reviewer']['deleted'],
                'Reviewer_FirstName': review['reviewer']['firstName'],
                'Reviewer_isSuperhost': review['reviewer']['isSuperhost'],
                'Reviewer_PictureURL': reviewer_pictureURL,
                'Reviewer_Region': reviewer_region,
                'Reviewer_Country': reviewer_country,
                'Reviewer_MonthsOnAirbnb': reviewer_months,
                'Reviewer_YearsOnAirbnb': reviewer_years,
                'Reviewer_NewToAirbnb': reviewer_new_to_airbnb,
                'Host_ID': review['reviewee']['id'],
                'Host_FirstName': review['reviewee']['firstName'],
                'Host_isSuperhost': review['reviewee']['isSuperhost'],
                'Host_PictureURL': host_pictureURL,
                'Host_Response': review.get('response'),
                'Host_Response_Localized': response_localized,
                'Host_isHighlightedReview': isHostHighlightedReview,
                'RecordInserted': json_data['RecordInserted']
            }
            
            rows.append(this_row_dict)
            
            ## Try to stop text bleed
            for key, value in this_row_dict.items():
                if isinstance(value, str):  # Apply only to string values
                    this_row_dict[key] = value.replace('\n', '\\n').replace('\r', '\\r')

        return pd.DataFrame(rows)
    
    def transform_AirbnbAmenities(self, data):
        """
        Transforms listing amenities from the Description JSON data into a pandas DataFrame.

        This function extracts all available amenity information from the listing's description

        Args:
            data (dict): Raw dict containing 'originalDescription'

        Returns:
            pd.DataFrame: DataFrame where each row represents a single available amenity for the listing
        """

        def amenities_clean_key(text: str) -> str:
            ## Remove system labels from Airbnb image icons
            text = re.sub(r"^SYSTEM_", "", text)
            text = re.sub(r"^MAPS_", "", text)
            return text

        rows = []

        ## Iterate through each amenity, and add as a row
        for section in data['originalDescription']['sections']:
            if section['sectionId'] == 'AMENITIES_DEFAULT':
                for i in section['section']['seeAllAmenitiesGroups']:
                    amenity_group = i['title']
                    for j in i['amenities']:
                        if j['available'] is False:
                            continue
                        
                        amenities_dict = {
                            'Country': self.ctx.country,
                            'Location': self.ctx.location,
                            'Airbnb_ListingID': data['originalDescription']['metadata']['loggingContext']['eventDataLogging']['listingId'],
                            'Scrape_Date': self.first_entry_date,
                            'Amenity_Group': amenity_group,
                            'Amenity_Key': amenities_clean_key(j['icon']),
                            'Amenity_Title': j['title'],
                            'Amenity_Subtitle': j['subtitle'],
                            'RecordInserted': data['RecordInserted']
                        }
                        
                        rows.append(amenities_dict)
            
        return pd.DataFrame(rows)
   
def extractPricingValue(price_string: str) -> int | None:
    """
    Extracts the numeric value from a price string and converts it to an integer.

    This function searches for numbers with optional commas (e.g., "1,200") in the input string,
    ignores decimal fractions, and returns the integer value. Returns None if no numeric value is found.

    Args:
        price_string (str): A string potentially containing a price or numeric value.

    Returns:
        int | None: The extracted integer value of the price, or None if extraction fails.
    """

    try:
        m = re.search(r"(\d{1,3}(?:,\d{3})*)(?:\.\d+)?", price_string)
    except:
        input(price_string)
    if m:
        num_str = m.group(1).replace(",", "")
        return int(num_str)
    return None
    
def textExtractRegex(full_string: str, search_string: str) -> str | None:
    """
    Extracts the first capture group from a string using a regular expression.

    Args:
        full_string (str): The string to search within.
        search_string (str): A regex pattern with at least one capture group.

    Returns:
        str | None: The first captured group if a match is found, otherwise None.
    """

    try:
        re.search(search_string, full_string).group(1)
    except:
        return None
    
if __name__ == '__main__':
    print("This is the Data handler. Run web_scraper.py instead")