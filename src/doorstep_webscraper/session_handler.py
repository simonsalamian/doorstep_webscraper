import requests
import random
import json
import base64
from datetime import datetime
from urllib.parse import urlencode

## Doorstep Analytics Scripts
from constants import URLS, USER_AGENTS
from utils import r_sleep, dict_subset

## Custom logging script
from config_logging import setup_logging
logger = setup_logging()


class SessionHandler:
    """ Handles all connections and requests to Airbnb APIs """
    
    def __init__(self, context):
        ## Initialize a session using the Python Requests module. Store Airbnb cookie data to avoid bot detection
        logger.info("Pinging Airbnb.com for inital cookies and session data")
        self.ctx = context
        self.session = requests.Session()
        
        ## Run two session requests to get cookies, to be applied to requests module to avoid detection
        self.session.get("https://www.airbnb.com", headers=self.randomHeaders())
        r_sleep(2)
        
        self.session.get(f'https://www.airbnb.com/s/{self.ctx.location}--United-Kingdom', headers=self.randomHeaders())
        r_sleep(2)
        
    def randomHeaders(self):    
        ## To reduce detection, use a random user_agent in the request header
        selected_user_agent = random.choice(USER_AGENTS)
        return {"User-Agent": selected_user_agent, "Accept": "*/*", "Accept-Language": "en-GB,en;q=0.5", "Accept-Encoding": "gzip, deflate","X-Airbnb-Supports-Airlock-V2": "true", "X-Airbnb-API-Key": "d306zoyjsyarp7ifhu67rjxn52tv0t20",
            "X-CSRF-Token": "null", "X-CSRF-Without-Token": "1", "X-Airbnb-GraphQL-Platform": "web","X-Airbnb-GraphQL-Platform-Client": "minimalist-niobe","X-Niobe-Short-Circuited": "true", "Origin": "https://www.airbnb.com","Sec-Fetch-Dest": "empty","Sec-Fetch-Mode": "cors","Sec-Fetch-Site": "same-origin","Alt-Used": "www.airbnb.com","TE": "trailers",}
        
    def createDataPayloadMapAPI(self, runner_type, coords, offset):
        """
        Generate the appropriate payload for different API map runners based on runner_type.
    
        Args:
            runner_type (str): Type of runner; must be one of 'explore', 'stays', or 'pricing'.
            coords : dict
                Dictionary of map boundary coordinates and zoom level. Required keys:
                    - 'neLat' (float): Northeast latitude
                    - 'neLng' (float): Northeast longitude
                    - 'swLat' (float): Southwest latitude
                    - 'swLng' (float): Southwest longitude
                    - 'zoom'  (int): Map zoom level used in the request
            offset (int): Offset value used for pagination. Varies by API
    
        Returns:
            dict: payload required for API Request to Airbnb
    
        Raises:
            ValueError: If runner_type is not one of the expected values.
        
        Behavior:
            - For 'explore', calls createExplorePayload with coords and offset.
            - For 'stays' and 'pricing', encodes pagination info and calls createPricingPayload.
        """
        
        if runner_type == 'explore':
            return self.createExplorePayload(coords, offset)
        elif runner_type in ['stays', 'pricing']:
            ## Pages are stored in base64 format when added to the API request
            if offset > 0:
                json_string = json.dumps({"section_offset":0,"items_offset":offset,"version":1}, separators=(",", ":"))
                pagination = base64.b64encode(json_string.encode('utf-8')).decode('utf-8')
            else:
                pagination = None
            return self.createPricingPayload(coords, pagination)
        else:
            raise ValueError(f"Invalid runner_type for createDataPayloadMapAPI: {runner_type}")
    
    def makeRequest(self, request_type, url, **kwargs):
        """
        Sends a GET or POST request to the specified Airbnb API URL with retry and error handling.
    
        Args:
            request_type (str): HTTP method to use, either 'get' or 'post'.
            url (str): The endpoint URL to send the request to, from URLS dict in constants.py
            **kwargs:
                dataPayload (dict, optional): JSON body for POST requests.
                params (dict or str, optional): Query parameters for GET requests.
                return_raw (bool, optional): If True, returns the raw response object instead of parsed JSON. Raw response primarily used for downloading images
    
        Returns:
            dict or requests.Response or None: Parsed JSON response (default), raw response object if `return_raw` is True, 
            or None if the request fails after multiple retries.
    
        Notes:
            - Automatically handles 429 (rate limit), 415, and 405 response codes with retry logic.
            - Retries up to 8 times on failure with increasing delay.
            - Random sleeps between retries using custom r_sleep().
        """

        dataPayload = kwargs.get('dataPayload')
        params = kwargs.get('params')
        return_raw = kwargs.get('return_raw')

        request_error_count = 0
        while True:
            
            ## Retry if request fails, up to 8 times
            if request_error_count >= 8:
                logger.warning("Request failed 8+ times. Skipping")
                r_sleep(3600)
                return None
            elif request_error_count >= 4:
                logger.info(f'Request number {request_error_count}')
                r_sleep(60*request_error_count)
            elif request_error_count > 0:
                logger.info(f'Request number {request_error_count}')
                r_sleep(10*request_error_count)
    
            try:
                if request_type == 'get':
                    response = self.session.get(url, headers=self.randomHeaders(), params=params, timeout=12)
                elif request_type == 'post':
                    response = self.session.post(url, headers=self.randomHeaders(), json=dataPayload, timeout=12)
    
                ## Network response error handling
                if response.status_code == 429:
                    logger.warning("Too many requests to server. Restart with a slower speed or use a VPN")
                    r_sleep(1200)
                    continue
    
                elif response.status_code == 415:
                    logger.warning("415 Media Error: Image or content does not exist")
                    break
    
                elif response.status_code == 405:
                    logger.warning("405 Error: No request output for method")
                    continue
    
                elif response.status_code != 200:
                    logger.warning(f"Response failure. Status code: {response.status_code}")
                    continue
    
                ## Success case (status_code == 200)
                if response.status_code == 200:
                    if request_error_count > 0:
                        logger.info(f'Successfully extracted data, attempt {request_error_count}')
                    if return_raw:
                        return response
                    else:
                        return json.loads(response.text)
    
            ## Additional network error handling
            except requests.exceptions.HTTPError as http_error:
                logger.warning(f"HTTP error occurred: {http_error}")
            except requests.exceptions.ConnectionError as connection_error:
                logger.warning(f"Connection error occurred: {connection_error}")
            except requests.exceptions.Timeout as timeout_error:
                logger.warning(f"Timeout error occurred: {timeout_error}")
            except Exception as e:
                logger.warning(f"An unexpected request error occurred: {e}")
            finally:
                request_error_count += 1
                r_sleep(0.28)
                
    def createExplorePayload(self, coords, offset):
        """
        Generates a structured payload for Airbnb map-based "Explore" API endpoint
    
        Parameters:
        ----------
        coords : dict
            Dictionary of map boundary coordinates and zoom level. Required keys:
                - 'neLat' (float): Northeast latitude
                - 'neLng' (float): Northeast longitude
                - 'swLat' (float): Southwest latitude
                - 'swLng' (float): Southwest longitude
                - 'zoom'  (int): Map zoom level used in the request

        offset : int
            Used for pagination. Represents the item offset for the current results page.
            Increases in lots of 18, where 18 == Page 1, 36 == Page 2 etc.
    
        Returns:
        -------
        dict: A dictionary representing the structured request payload, ready for Airbnb endpoint.
        """
        
        return {
            "variables": {
                "request": {
                    "metadataOnly": "False",
                    "version": "1.8.3",
                    "itemsPerGrid": 18,
                    "tabId": "home_tab",
                    "refinementPaths": ["/homes"],
                    "source": "structured_search_input_header",
                    "searchType": "pagination",
                    "mapToggle": "False",
                    "neLat": coords["neLat"],
                    "neLng": coords["neLng"],
                    "swLat": coords["swLat"],
                    "swLng": coords["swLng"],
                    "searchByMap": "true",
                    "itemsOffset": offset,
                    "cdnCacheSafe": "False",
                    "simpleSearchTreatment": "simple_search_only",
                    "treatmentFlags": [],
                    "screenSize": "large",
                    "zoomLevel": coords["zoom"]
                }
            },
            "extensions": {
                "persistedQuery": {
                    "version": 1,
                    "sha256Hash": "647ecde501ef18a6096e0bc1d41ed24b74aba0d99c072b34d84660ada41988f0"
                }
            }
        }
    
    def createPropertyDetailPayload(self, listing_id):
        """
        Construct the URL-encoded query string payload for fetching additional stay-related sections
        from the Airbnb PDP (Property Detail Page) API.
    
        Args:
            listing_id (int): Unique identifier for the listing.
    
        Returns:
            str: URL-encoded query string with embedded variables and extensions.
        """
        
        listingid_encoded = base64.b64encode(f"StayListing:{listing_id}".encode('utf-8')).decode('utf-8')
    
        variables = {
            "id": listingid_encoded,
            "pdpSectionsRequest": {
                "adults": None,
                "bypassTargetings": False,
                "categoryTag": None,
                "causeId": None,
                "children": None,
                "disasterId": None,
                "discountedGuestFeeVersion": None,
                "displayExtensions": None,
                "federatedSearchId": None,
                "forceBoostPriorityMessageType": None,
                "infants": None,
                "interactionType": None,
                "layouts": ["SIDEBAR", "SINGLE_COLUMN"],
                "pets": 0,
                "pdpTypeOverride": None,
                "photoId": None,
                "preview": False,
                "previousStateCheckIn": None,
                "previousStateCheckOut": None,
                "priceDropSource": None,
                "privateBooking": False,
                "promotionUuid": None,
                "relaxedAmenityIds": None,
                "searchId": None,
                "selectedCancellationPolicyId": None,
                "selectedRatePlanId": None,
                "splitStays": None,
                "staysBookingMigrationEnabled": False,
                "translateUgc": None,
                "useNewSectionWrapperApi": False,
                "sectionIds": [
                    "BOOK_IT_CALENDAR_SHEET",
                    "CANCELLATION_POLICY_PICKER_MODAL",
                    "POLICIES_DEFAULT",
                    "BOOK_IT_SIDEBAR",
                    "URGENCY_COMMITMENT_SIDEBAR",
                    "BOOK_IT_NAV",
                    "BOOK_IT_FLOATING_FOOTER",
                    "EDUCATION_FOOTER_BANNER",
                    "URGENCY_COMMITMENT",
                    "EDUCATION_FOOTER_BANNER_MODAL"
                ],
                "checkIn": None,
                "checkOut": None,
                "p3ImpressionId": "p3_1705930404_zNFUjTYJni8B6Lvb"
            }
        }
    
        extensions = {
            "persistedQuery": {
                "version": 1,
                "sha256Hash": "37d7cbb631196506c3990783fe194d81432d0fbf7362c668e547bb6475e71b37"
            }
        }
    
        query_params = {
            "operationName": "StaysPdpSections",
            "locale": "en-GB",
            "currency": "USD",
            "variables": json.dumps(variables, separators=(",", ":")),
            "extensions": json.dumps(extensions, separators=(",", ":"))
        }
    
        return urlencode(query_params)
    
    def createPricingPayload(self, coords, pagination):
        """
        Constructs the request payload for Airbnb pricing API based on map coordinates and optional pagination.

        This payload requires coords to call the Map API, optionally the number of adults and
        check-in/check-out dates can be included. If no dates are provided, Airbnb will provide pricing for the next
        available five day period within the next two months. If no adults are set, defaults to 1 (for Stays API)

        Args:
            coords (dict): A dictionary containing the map tile coordinates and zoom level, with keys:
                - 'neLat', 'neLng', 'swLat', 'swLng', 'zoom'.
            pagination (str | None): A cursor string for paginated results; None for first page.

        Returns:
            dict: A structured dictionary representing the JSON payload for the Airbnb Stays API.
        """

        ## Set default adults to 1 if not set
        if self.adults is None:
            adults = 2
        else:
            adults = self.adults

        raw_params = [
            {"filterName": "cdnCacheSafe", "filterValues": ["False"]},
            {"filterName": "channel", "filterValues": ["EXPLORE"]},
            {"filterName": "datePickerType", "filterValues": ["calendar"]},
            {"filterName": "flexibleTripLengths", "filterValues": ["one_week"]},
            {"filterName": "guests", "filterValues": [str(adults)]},
            {"filterName": "itemsPerGrid", "filterValues": ["18"]},
            {"filterName": "neLat", "filterValues": [str(coords["neLat"])]},
            {"filterName": "neLng", "filterValues": [str(coords["neLng"])]},
            {"filterName": "refinementPaths", "filterValues": ["/homes"]},
            {"filterName": "screenSize", "filterValues": ["large"]},
            {"filterName": "searchByMap", "filterValues": ["true"]},
            {"filterName": "searchMode", "filterValues": ["regular_search"]},
            {"filterName": "swLat", "filterValues": [str(coords["swLat"])]},
            {"filterName": "swLng", "filterValues": [str(coords["swLng"])]},
            {"filterName": "tabId", "filterValues": ["home_tab"]},
            {"filterName": "version", "filterValues": ["1.8.3"]},
            {"filterName": "zoomLevel", "filterValues": [str(coords["zoom"])]},
        ]

        # Only add checkin/checkout if provided
        if self.check_in:
            raw_params.append({"filterName": "checkin", "filterValues": [self.check_in]})
        if self.check_out:
            raw_params.append({"filterName": "checkout", "filterValues": [self.check_out]})

        base_request = {
            **({"cursor": pagination} if pagination else {}),
            "metadataOnly": False,
            "requestedPageType": "STAYS_SEARCH",
            "searchType": "user_map_move",
            "treatmentFlags": [
                "feed_map_decouple_m11_treatment",
                "recommended_amenities_2024_treatment_b",
                "filter_redesign_2024_treatment",
                "filter_reordering_2024_roomtype_treatment",
                "p2_category_bar_removal_treatment",
                "selected_filters_2024_treatment",
                "recommended_filters_2024_treatment_b",
                "m13_search_input_phase2_treatment",
                "m13_search_input_services_enabled",
            ],
            "skipHydrationListingIds": [],
            "maxMapItems": 9999,
            "rawParams": raw_params,
        }

        return {
            "operationName": "StaysSearch",
            "variables": {
                "staysSearchRequest": base_request,
                "staysMapSearchRequestV2": base_request,  ## merged, no duplication
                "isLeanTreatment": False,
                "aiSearchEnabled": False,
                "skipExtendedSearchParams": False,
            },
            "extensions": {
                "persistedQuery": {
                    "version": 1,
                    "sha256Hash": "2a2757f965a334843f2d14f392f04e632b66189e4db892c9ca8b181f7614c7ba",
                }
            },
        }
     
    def createDescriptionPayload(self, listing_id, translate=False):
        """
       Construct the payload to fetch the property description.
    
       Args:
           listing_id (int): The unique identifier for the listing.
           translate (bool): Whether to request Airbnb content translation. Defaults to False.
                             Should be used for all locations in countries not in DO_NOT_TRANSLATE
       Returns:
           dict: A payload dictionary ready to be sent in to the Airbnb API.
       Notes:
           The listing ID is encoded as a base64 string prefixed with 'StayListing:'
       """
       
        listing_id_encoded = base64.b64encode(f'StayListing:{listing_id}'.encode('utf-8')).decode('utf-8')
        return {
            "operationName": "StaysPdpSections",
            "locale": "en-GB",
            "currency": "USD",
            "variables": {
                "id": listing_id_encoded,
                "pdpSectionsRequest": {
                    "adults": "2",
                    "amenityFilters": None,
                    "bypassTargetings": False,
                    "categoryTag": None,
                    "causeId": None,
                    "children": "0",
                    "disasterId": None,
                    "discountedGuestFeeVersion": None,
                    "displayExtensions": None,
                    "forceBoostPriorityMessageType": None,
                    "infants": "0",
                    "interactionType": None,
                    "layouts": ["SIDEBAR", "SINGLE_COLUMN"],
                    "pets": 0,
                    "pdpTypeOverride": None,
                    "photoId": None,
                    "preview": False,
                    "previousStateCheckIn": None,
                    "previousStateCheckOut": None,
                    "priceDropSource": None,
                    "privateBooking": False,
                    "promotionUuid": None,
                    "relaxedAmenityIds": None,
                    "searchId": None,
                    "selectedCancellationPolicyId": None,
                    "selectedRatePlanId": None,
                    "splitStays": None,
                    "staysBookingMigrationEnabled": False,
                    "translateUgc": translate,
                    "useNewSectionWrapperApi": False,
                    "sectionIds": None,
                }
            },
            "extensions": {
                "persistedQuery": {
                    "version": 1,
                    "sha256Hash": "08e3ad2e3d75c9bede923485718ff2e7f6efe2ca1febb5192d78c51e17e8b4ca",
                }
            }
        }

    def createReviewsPayload(self, listing_id, offset):
        """
        Construct the payload to fetch reviews for a given listing.
        Reviews are paginated, use an offset (in lots of 24) to get a page of reviews
    
        Args:
            listing_id (int): The unique identifier for the listing.
            offset (int): Pagination offset for reviews, in increases of 24
                          Where 24 == Page 1, 48 == Page 2 etc.
        Returns:
            dict: Payload dictionary for the 'StaysPdpReviews' GraphQL operation.
        Notes:
            Requests up to 24 reviews sorted by most recent
        """
        
        listing_id_encoded = base64.b64encode(f'StayListing:{listing_id}'.encode('utf-8')).decode('utf-8')
        return {
            "operationName": "StaysPdpReviews",
            "locale": "en-GB",
            "currency": "USD",
            "variables": {
                "id": listing_id_encoded,
                "pdpReviewsRequest": {
                    "fieldSelector": "for_p3_translation_only",
                    "forPreview": False,
                    "limit": 24,
                    "offset": offset,
                    "showingTranslationButton": False,
                    "first": 24,
                    "sortingPreference": "MOST_RECENT",
                    "after": None,
                },
            },
            "extensions": {
                "persistedQuery": {
                    "version": 1,
                    "sha256Hash": "a4f5f2155e9b713d0061e170b6aad790a46fae542af85cb68fb58d0a1c9472ff",
                }
            }
        }
        
    def createCalendarPayload(self, listing_id):
        """
        Build the payload to fetch availability calendar data for a listing.

        Args:
            listing_id (int): The unique identifier of the listing
        Returns:
            dict: Payload dictionary for the 'PdpAvailabilityCalendar' API
        Notes:
            Requests availability data for 12 months starting from the current month and year
        """
        
        now = datetime.now()
        return {
            "operationName": "PdpAvailabilityCalendar",
            "locale": "en-GB",
            "currency": "USD",
            "variables": {
                "request": {
                    "count": 12,
                    "listingId": listing_id,
                    "month": now.month,
                    "year": now.year,
                }
            },
            "extensions": {
                "persistedQuery": {
                    "version": 1,
                    "sha256Hash": "8f08e03c7bd16fcad3c92a3592c19a8b559a0d0855a84028d1163d4733ed9ade",
                }
            }
        }
    
    def scrapeCalendarToFile(self, listing_id):
        """
        Scrape the availability calendar for a specific Airbnb listing and save it as a JSON file.
    
        Args:
            listing_id (int): The unique identifier of the Airbnb listing.
    
        Behavior:
            - Constructs the payload using `createDataPayloadListingAPI` with the 'calendar' runner type.
            - Sends a POST request to the Airbnb calendar endpoint.
            - Parses the response to extract future availability data from the `calendarMonths` section.
            - Each day includes availability status, bookability, and minimum/maximum stay limits.
            - Filters out past dates to only retain future availability.
            - Saves the result as a JSON file using the `file_mgr` to'calendar' folder
        """
        
        dataPayload = self.createCalendarPayload(listing_id)
        response = self.makeRequest(request_type='post', url=URLS['calendar'], dataPayload=dataPayload)
        
        ## c represents all calendar data, where each month is a new list within a list
        c = dict_subset(response, 'data', 'merlin', 'pdpAvailabilityCalendar', 'calendarMonths')
        if c is not None:
            ## Store all days inside day_list
            day_list = []
            
            for month in c:
                for day in month['days']:
                    if datetime.strptime(day['calendarDate'], "%Y-%m-%d").date() > datetime.now().date():
                            day_dict = {'Calendar_Date': day['calendarDate'], 'isAvailable': day['available'],
                                'Min_Nights': day['minNights'], 'Max_Nights': day['maxNights'],
                                'Available_For_Checkin': day['availableForCheckin'],'Available_For_Checkout': day['availableForCheckout'],
                                'isBookable': day['bookable'] }
                            day_list.append(day_dict)
            
            ## Create a clean dict with metadata, and save each day's availability as a list element
            c_dict = {'Date_of_Data_Collection': datetime.now().strftime("%Y-%m-%d"), 'ListingID': listing_id,'Calendar': day_list}
            self.ctx.file_mgr.saveJSONFile(c_dict, 'calendar', listing_id)
            
    def extractPricingToFile(self, e, listing_id):
        """
        Extract pricing information from the Pricing API response and save it to a JSON file.
    
        Args:
            e (dict): The dict associated with a particular listing.
            listing_id (int): The unique identifier of the listing being scraped.
    
        Behavior:
            - Builds a `pricing_dict` containing:
                - Check-in and check-out dates.
                - Week label for grouping.
                - Adult count.
                - `listingParamOverrides` and `pricingQuote` (if available in `e`).
            - Attempts to read the existing pricing JSON file for the listing.
                - On first run, the file will not exist, so initialize with basic metadata.
            - Prevents duplication:
                - Compares existing entries by `start_date` and number of guests.
                - If a matching or newer entry exists, the method exits early without writing.
            - If no duplicate is found, appends the new `pricing_dict` to the `prices` list and writes it back to file.
            - Pricing entries are stored under a single JSON file per listing.
        """
        
        ## Add basic details to pricing element
        pricing_dict = {'start_date': self.check_in, 'end_date': self.check_out, 'week_label': self.week_label, 'adults': self.adults}
        pricing_dict['structuredDisplayPrice'] = dict_subset(e, 'structuredDisplayPrice')
        
        ## All API calls go to one pricing file. If the file exists, append to dict "existing_json"
        existing_json = self.ctx.file_mgr.readJSONFile('pricing', listing_id)
        if existing_json is None:
            existing_json = {'listingid': listing_id, 'currency': self.ctx.currency, 'scrape_datetime': datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'prices': []}
        
        ## Check not duplicating prices:
        current_start_date = datetime.strptime(self.check_in, '%Y-%m-%d')
        if existing_json['prices']:
            for price in existing_json['prices']:
                price_start_date = datetime.strptime(price['start_date'], '%Y-%m-%d')
                guests = price['adults']
                
                # If any start_date is greater than current_start_date, return None
                if price_start_date >= current_start_date and guests == self.adults:
                    return
                
        ## Append latest data to exisiting_json and save
        existing_json['prices'].append(pricing_dict)
        self.ctx.file_mgr.saveJSONFile(existing_json, 'pricing', listing_id)
        
    def scrapeDescriptionToFile(self, listing_id, translate=False):
        """
        Scrape the property description for a given listing and save it as a JSON file.
    
        Args:
            listing_id (int): The unique identifier of the Airbnb listing.
            translate (bool): Whether to fetch the translated (English) version of the description. Defaults to False.
    
        Behavior:
            - Sends a POST request to the description endpoint using a payload created by `createDescriptionPayload()`.
            - Saves the data to a JSON file in the 'description' folder.
    
        File Structure:
            - If `translate` is False:
                - Creates or overwrites the file with a single key: `"originalDescription"`.
            - If `translate` is True:
                - Reads the existing file.
                - Adds `"translatedDescription"` as a new key alongside the original text.
                - Overwrites the file with both original and translated versions.
    
        Notes:
            - Ensures that the untranslated description is saved before any translation is appended
        """
        
        dataPayload = self.createDescriptionPayload(listing_id, translate=translate)
        response = self.makeRequest(request_type='post', url=URLS['description'], dataPayload=dataPayload)
        
        ## d represents all useful description data
        d = dict_subset(response, 'data', 'presentation', 'stayProductDetailPage', 'sections')
        
        ## Two dicts within Description file, one for the Original text (untranslated) 
        ## and one for Translated (in English). Original text is always saved first
        if not translate:
            self.ctx.file_mgr.saveJSONFile({'originalDescription': d}, 'description', listing_id)
        else:
            t = self.ctx.file_mgr.readJSONFile('description', listing_id)
            t['translatedDescription'] = d
            self.ctx.file_mgr.saveJSONFile(t, 'description', listing_id)
            
    def scrapeReviewsToFile(self, e, listing_id):
        """
        Scrape all available reviews for a given listing and save them to a JSON file.
    
        Args:
            e (dict): Parsed metadata from a previous API response (used to extract review count).
            listing_id (str or int): The unique identifier of the listing.
        Behavior:
            - Extracts the total number of reviews from either the 'listing_stays' or 'reviewsCount' keys.
            - Iterates through paginated review data using a fixed page size of 24.
            - On each page:
                - Sends a POST request to the reviews endpoint with the correct offset.
                - Extracts review entries and appends them to a master list.
                - If running in free/preview mode (no `paid_premium_code`), limits to 20 reviews per listing.
            - Sleeps briefly between requests to avoid rate-limiting.
            - Saves the final review data to a JSON file in the 'reviews' folder.
        Notes:
            - The offset logic increments by 24 per page, consistent with Airbnb's API pagination.
        Limitations:
            - No deduplication or retry logic is implemented here; assumes API responses are consistent.
        """
    
        ## Get total review counts from either Stays API or Explore API.
        ## Required to know when to stop iterating through review pages
        if dict_subset(e, 'listing_stays') is not None:
            reviewsCount = int(dict_subset(e, 'sections', 'metadata', 'sharingConfig', 'reviewCount'))
        elif dict_subset(e, 'reviewsCount') is not None:
            reviewsCount = int(dict_subset(e, 'reviewsCount'))
        elif dict_subset(e, 'metadata', 'sharingConfig', 'reviewCount') is not None:
            reviewsCount = int(dict_subset(e, 'metadata', 'sharingConfig', 'reviewCount'))
        
        ## Iterate through each page of reviews, add the review details as a dict to reviews_list
        ## Offset represents the page number, with each page containing 24 listings. Offset increases in lots of 24
        reviews_list = []
        offset = 0
        
        while offset < reviewsCount:
            dataPayload = self.createReviewsPayload(listing_id, offset=offset)
            response = self.makeRequest(request_type='post', url=URLS['reviews'], dataPayload=dataPayload)
            
            r = dict_subset(response, 'data', 'presentation', 'stayProductDetailPage', 'reviews', 'reviews')
            
            ## Convert None to an empty list to avoid error, and add each review to reviews_list
            r = [] if r is None else r
            for review in r:
                reviews_list.append(review)

                if len(reviews_list) >= 20 and self.ctx.isWebPreview:
                    offset += 99999  ## Hacky. Force offset > reviewsCount
                    break

            r_sleep(0.18)
            offset += 24

        reviews_dict = {'Date_of_Data_Collection': datetime.now().strftime("%Y-%m-%d"), 'ListingID': listing_id, 'reviews': reviews_list }
        self.ctx.file_mgr.saveJSONFile(reviews_dict, 'reviews', listing_id)

if __name__ == '__main__':
    print("This is the session handler. Run web_scraper.py instead")