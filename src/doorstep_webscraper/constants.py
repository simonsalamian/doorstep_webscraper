## constants.py
""" 
Airbnb API URLs called by the script, and a selection of User Agents to diversify calls and reduce 429 Too Many Requests errors
"""

URLS = {
    ## These URLs are taken from Airbnb's web application, including hard-coded hash fragments
    ## They may be subject to change at any time
        
    ## ExploreSearch is the old Airbnb Map API, it still works to produce ~98% of available listings
    "explore": "https://www.airbnb.com/api/v3/ExploreSearch?operationName=ExploreSearch&locale=en-GB",
    
    ## Stays is the current Airbnb Map API, used to append basic pricing data and fill in missing 2% of missing listings above
    "stays": "https://www.airbnb.com/api/v3/StaysSearch/910bfdc30d4cb9e84c1661dc34e5f1d0747cf689defcf900c146d1762722a7ae?operationName=StaysSearch&locale=en-GB",

    ## Listing Details, used to append all listing data for missing 5% above
    ## NO LONGER USED
    "listing_details": "https://www.airbnb.com/api/v3/StaysPdpSections/37d7cbb631196506c3990783fe194d81432d0fbf7362c668e547bb6475e71b37",
    
    ## The same as the above Stays API, uses prices label in this script to differentiate when dates are added
    "pricing": "https://www.airbnb.com/api/v3/StaysSearch/910bfdc30d4cb9e84c1661dc34e5f1d0747cf689defcf900c146d1762722a7ae?operationName=StaysSearch&locale=en-GB",

    ## Data from the Airbnb listing page
    "description": "https://www.airbnb.co.uk/api/v3/StaysPdpSections/08e3ad2e3d75c9bede923485718ff2e7f6efe2ca1febb5192d78c51e17e8b4ca",

    ## Calendar availability API, accessed by scrolling through dates when booking on Airbnb
    "calendar": "https://www.airbnb.com/api/v3/StaysPdpSections",

    ## Review data, accessed by scrolling through reviews on a listing
    "reviews": "https://www.airbnb.co.uk/api/v3/StaysPdpReviews/a4f5f2155e9b713d0061e170b6aad790a46fae542af85cb68fb58d0a1c9472ff"
}


USER_AGENTS = [
    ## Common user agents to randomize scraping headers
    "chrome|Mozilla/5.0 (Windows NT 10.1;) AppleWebKit/602.30 (KHTML, like Gecko) Chrome/51.0.2508.225 Safari/536",
    "chrome|Mozilla/5.0 (Windows; Windows NT 6.0;; en-US) AppleWebKit/536.24 (KHTML, like Gecko) Chrome/50.0.1631.376 Safari/600",
    "chrome|Mozilla/5.0 (Linux; U; Linux x86_64; en-US) AppleWebKit/536.44 (KHTML, like Gecko) Chrome/49.0.3540.303 Safari/602",
    "chrome|Mozilla/5.0 (Windows; U; Windows NT 6.0; Win64; x64) AppleWebKit/600.33 (KHTML, like Gecko) Chrome/55.0.1654.327 Safari/601",
    "chrome|Mozilla/5.0 (Macintosh; U; Intel Mac OS X 8_9_7) AppleWebKit/602.38 (KHTML, like Gecko) Chrome/50.0.2564.307 Safari/534",
    "chrome|Mozilla/5.0 (Linux; Linux i646 ) AppleWebKit/533.23 (KHTML, like Gecko) Chrome/54.0.2382.290 Safari/600",
    "chrome|Mozilla/5.0 (Linux; Linux x86_64) AppleWebKit/534.31 (KHTML, like Gecko) Chrome/52.0.1997.334 Safari/536",
    "chrome|Mozilla/5.0 (Linux; Linux i651 x86_64) AppleWebKit/533.41 (KHTML, like Gecko) Chrome/49.0.3625.119 Safari/534",
    "chrome|Mozilla/5.0 (Macintosh; U; Intel Mac OS X 9_3_4) AppleWebKit/601.37 (KHTML, like Gecko) Chrome/51.0.1849.368 Safari/603",
    "chrome|Mozilla/5.0 (Windows; Windows NT 6.2; Win64; x64) AppleWebKit/533.11 (KHTML, like Gecko) Chrome/52.0.3026.385 Safari/600",
    "firefox|Mozilla/5.0 (Windows; Windows NT 10.2; Win64; x64; en-US) Gecko/20130401 Firefox/69.1",
    "firefox|Mozilla/5.0 (Macintosh; U; Intel Mac OS X 9_2_0) Gecko/20130401 Firefox/45.1",
    "firefox|Mozilla/5.0 (Linux i661 ; en-US) Gecko/20100101 Firefox/54.8",
    "firefox|Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_4_3; en-US) Gecko/20100101 Firefox/60.3",
    "firefox|Mozilla/5.0 (Windows NT 10.0; Win64; x64; en-US) Gecko/20100101 Firefox/70.1",
    "firefox|Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_3_3) Gecko/20100101 Firefox/60.7",
    "firefox|Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_6) Gecko/20130401 Firefox/60.2",
    "firefox|Mozilla/5.0 (Macintosh; U; Intel Mac OS X 7_6_2) Gecko/20100101 Firefox/69.7",
    "firefox|Mozilla/5.0 (Windows; U; Windows NT 10.2; Win64; x64) Gecko/20100101 Firefox/68.0",
    "firefox|Mozilla/5.0 (Linux; Linux x86_64; en-US) Gecko/20100101 Firefox/61.2",
]

## For countries with English as a first language, do not waste resources translating descriptions to English
DO_NOT_TRANSLATE = [
    'UK',
    'Singapore',
    'Thailand',
    'Vietnam',
    'Australia',
    'Ireland',
    'New Zealand',
    'Malaysia',
    'US',
    'Canada',
    'South Africa'
]