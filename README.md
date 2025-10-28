# Doorstep Analytics: Airbnb Webscraper
A Python-based scraper that collects Airbnb listing data using Airbnb’s internal APIs.
This tool is designed to run on a private GCP platform, but can be modified for analytics and personal projects.

For quick access to previously generated data for popular tourist destinations, see Doorstep Analytics: [doorstepanalytics.com](https://doorstepanalytics.com)

## Data Outputs
Six different data sets can be generated for any location:
- Overview: listing titles, property sizes, host info, review scores etc.
- Calendar: availability per listing per day, for the next 365 days
- Pricing: price per set time period (Mon-Fri or Fri-Sun) for different guests, if the listing is available on this date
- Description: Text descriptions from each listing
- Reviews: All reviews left on the listing, and host responses
- Amenities: Every amenity listed by the Airbnb host

## Usage
Run *src/doorstep_webscraper/web_scraper.py* to start

This web scraper is configured to use private GCP tables providing input co-ordinates and output tables. For local usage, GCP table functions must be disabled and the location co-ordinates obtained
using the function gcp_manager.get_customMapTileList()

This function rquires a browser URL that must be copied from Airbnb.com. The URL is accessed by performing any text based search from the homepage (eg: Edinburgh). By default, Airbnb will search by text, giving a URL such as:
```
https://www.airbnb.com/s/Edinburgh--United-Kingdom/homes?...&search_type=autocomplete_click
```
However the scraper needs to search by map movement, to get location co-ordinates. It is required that the user drags and drops and zooms the Airbnb map search to the desired location. This will update the browser URL to include the variables
**ne_lat, ne_lng, sw_lat, sw_lat and zoom**.
```
https://www.airbnb.com/s/Edinburgh--United-Kingdom/homes?...&ne_lat=55.97917790859959&**ne_lng=-3.1525942947892815&sw_lat=55.90448064534887&sw_lng=-3.2695066079750745&zoom=12.93545887779647&search_by_map=true
```

This URL can then be pasted into the scraper.

## Config
The config.toml file can allow or disallow the above datasets, to improve scrape speed:
```
scrape_calendar = True
scrape_weekly_pricing = True
scrape_description = True
translate_description_to_English = True
scrape_reviews = True
```

The option *isWebPreview* will only download 50 calendar, description and review files, and limited pricing files. This is set to True for the web updating service, but should be set to false for full dataset downloads.

A gcp_constants file provides private Google Cloud table and storage paths. This is not included in this public repository.

## Installation
**Clone the repository:** git clone https://github.com/simonsalamian/doorstep_webscraper.git

**Install dependencies:** pip install -r requirements.txt

## Requirements
Python 3.9+

requests – for API calls

pandas – for data handling and CSV export

tomllib - for config file

scikit-learn - for ML predictions (kNN modelling)

python-dotenv - loading environment variables

## Requirements for GCP access only

google-cloud-bigquery  

google-cloud-storage 

google-cloud-logging 

google-cloud-dataform

pandas-gbq - writing pandas dataframes to BigQuery

## Notes
This project is not affiliated with Airbnb in any way. Airbnb may change their API names and output at any time.

Use responsibly and respect Airbnb’s terms of service.

Heavy scraping may result in IP blocks — while the scraper does contain rate-limiting, you may wish to consider using a VPN

## License

This project is licensed under the GNU General Public License v3.0 (GPL-3.0).
See the LICENSE file for details.

## Authors

Simon Salamian – Developer and maintainer (simonsalamian@gmail.com)

Output published to Doorstep Analytics (https://doorstepanalytics.com)

