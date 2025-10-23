import os
import json
from datetime import datetime
import os
import sys
import subprocess
import zipfile
import tarfile
from io import BytesIO

## Custom logging script
from config_logging import setup_logging
logger = setup_logging()


class FileManager:
    """
    Deals with saving API data to JSON on local disk, and reading JSON data
    """

    def __init__(self, context):
        self.ctx = context
    
    def saveJSONFile(self, data: dict, folder: str, listing_id: str):
        """
        Saves a dictionary as a JSON file in the specified folder with the given listing ID.
        Adds a 'RecordInserted' timestamp to the data to track when it was saved.

        Args:
            data (dict): The data to save as JSON.
            folder (str): The subfolder within the output folder where the JSON file will be saved, eg Calendar, Overview
            listing_id (str): The listing ID is the filename (without extension) for the JSON file.
        """

        path = os.path.join(self.ctx.output_folder, folder, f"{listing_id}.json")

        ## Add a record inserted field, for clarity during data processing
        data['RecordInserted'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f)
            
    def readJSONFile(self, folder, listing_id):
        """
        Reads a JSON file for a given listing ID from a specified folder.

        Args:
            folder (str): The subfolder within the output folder where the JSON file is stored, eg Calendar, Overview
            listing_id (str): The listing ID, which corresponds to the JSON filename

        Returns:
            Parsed JSON data as a dict
        """

        if listing_id.endswith('.json'):
            listing_id = os.path.splitext(listing_id)[0]

        filepath = f'{self.ctx.output_folder}/{folder}/{listing_id}.json'
        try:
            with open(filepath, 'r') as file:
                return json.load(file)
        except FileNotFoundError:   ## Expected behaviour for pricing run
            return None
        except json.JSONDecodeError as e:
            logger.warning(f"Invalid JSON in file {filepath}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unknown error reading JSON file {filepath}: {e}")
            return None
                
    def listJSONFilesInFolder(self, folder):
        """ Returns a list of all JSON files in the given folder """
        base_path = os.path.dirname(os.path.dirname(folder))  # go up two folders
        target_path = os.path.join(base_path, "data", self.ctx.location, "overview")
        return [
            os.path.splitext(filename)[0]
            for filename in os.listdir(target_path)
            if filename.endswith(".json")
        ]
    
    def JSONFileDataGenerator(self, folder):
        """ Use generator to yield each JSON file, to avoid loading all into memory at once """
        folder_path = os.path.join(self.ctx.output_folder, folder)
        
        for filename in os.listdir(folder_path):
            file_path = os.path.join(folder_path, filename)
            
            with open(file_path, 'r', encoding='utf-8') as file:
                yield json.load(file)
                
    def CombineCSVchunksToOneFile(self, csv_output_filename, csv_files):
        """
        Combines multiple CSV files (chunks) into a single CSV file.
        The first CSV file's header is preserved, and all subsequent headers from other files
        are skipped. All rows from each file are appended in order.

        Args:
            csv_output_filename (str): The path to the output CSV file to create.
            csv_files (list[str]): List of CSV file paths to combine, in order.
        """
        
        logger.info(f"Combining CSV chunks to {csv_output_filename}")

        with open(csv_output_filename, 'w', newline='', encoding='utf-8') as outfile:
            with open(csv_files[0], 'r', encoding='utf-8') as infile:
                header = infile.readline()
                outfile.write(header)
        
            with open(csv_files[0], 'r', encoding='utf-8') as infile:
                next(infile)  ## Skip header
                for line in infile:
                    outfile.write(line)
        
            for file in csv_files[1:]:
                with open(file, 'r', encoding='utf-8') as infile:
                    next(infile) ## Skip header
                    for line in infile:
                        outfile.write(line)

    def openCSVWithDefault(self, csv_output_filename):
        """
        Open the completed CSV file (csv_output_filename) on the desktop. Not tested with Mac or Linux
        """
        full_path = os.path.join(os.getcwd(), csv_output_filename)
        if sys.platform.startswith("darwin"):  ## Mac
            subprocess.Popen(["open", full_path])
        elif os.name == "nt":       ## Windows
            os.startfile(full_path)
        elif os.name == "posix":    ## Linux
            subprocess.Popen(["xdg-open", full_path])
    
    def Zip_CSVfile(self, runner_type, csv_file_name, zip_file_name):
        logger.info(f'Zipping CSV file for {runner_type}')
        
        ## Zip CSV file
        zip_buffer = BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zipf:
            with open(csv_file_name, 'rb') as f:
                file_data = f.read()
                arcname = os.path.basename(csv_file_name)
                zipf.writestr(arcname, file_data)
        zip_buffer.seek(0)
        
        with open(zip_file_name, 'wb') as f:
            f.write(zip_buffer.getvalue())

    def ZipAllPreviewFiles_ToCloud(self):        
        csv_files = [
            os.path.join(self.ctx.output_folder, file)
            for file in os.listdir(self.ctx.output_folder)
            if (
                file.endswith(".csv") and (
                    file.startswith(f"DoorstepAnalyticsPreview_{self.ctx.location}")
                    or file.startswith(f"DoorstepAnalytics_{self.ctx.location}")
                )
            )
        ]

        zip_filename = f'{self.ctx.output_folder}/DoorstepAnalyticsPreview_{self.ctx.location}_{self.ctx.country}.zip'
        logger.info(f'Zipping preview files: {zip_filename}')
        
        # Include DataDictionary.xlsx if it exists
        data_dict_path = "files/DoorstepAnalytics_DataDictionary.xlsx"
        files_to_zip = csv_files + ([data_dict_path] if os.path.exists(data_dict_path) else [])
    
        # Create a zip file and add all the selected CSV files
        with zipfile.ZipFile(zip_filename, 'w') as zipf:
            for file in files_to_zip:
                zipf.write(file, arcname=os.path.basename(file))
                
        self.ctx.gcp_manager.pushZipToCloud(zip_filename, 'preview')
        
        ## Clean up files
        #for file in csv_files:
        #    os.remove(file)
        #os.remove(zip_filename)

    def BackupFiles_ToTarFile_ToCloud(self):        
        archive_tar_filename = f"{self.ctx.location}_{self.ctx.country}_{self.ctx.scrape_date_str}.tar.gz"
        
        logger.info(f'Creating file {archive_tar_filename}')    
        file_counter = 0
        
        with tarfile.open(archive_tar_filename, 'w:gz') as tar_gz_file:
            for root, dirs, files in os.walk(self.ctx.output_folder):
                for file in files:
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, self.ctx.output_folder)
                    tar_gz_file.add(file_path, arcname=arcname)
                    file_counter += 1
                    if file_counter % 10000 == 0:
                       logger.info(f"Zip added {file_counter} files to archive")        
        
        self.ctx.gcp_manager.pushArchiveToCloud(archive_tar_filename)
        os.remove(archive_tar_filename)
                        
if __name__ == '__main__':
    print("This is the file handler. Run web_scraper.py instead")