from sklearn import preprocessing
from sklearn.model_selection import train_test_split
from sklearn.neighbors import KNeighborsClassifier
from sklearn.model_selection import GridSearchCV
from sklearn import metrics
import numpy as np
import pandas as pd

from gcp_constants import GCP_BIGQUERY_TABLES
from config_logging import setup_logging
logger = setup_logging()

le = preprocessing.LabelEncoder()

class Neighbourhood:
    """
    Class for predicting missing Neighbourhood fields, using kNN ML modelling
    """
    def __init__(self, context):
        self.ctx = context
        self.matchNeighbourhoods()
    
    def matchNeighbourhoods(self):
        """
        Main runner for Neighbourhood ML processing. Runs the following steps:

        1.  Get a list of all neighbourhoods, and the corresponding latitude and longitude,
            for each listing. This is a cleaned dataset, removing Neighbourhood names that
            correspond to the location name, for example. If no neighbourhood labels are 
            provided at all (for very small locations), return None and exit

        2.  Create the ML model using currently available Neighbourhood field data. Use a
            test-train process to report expected model accuracy

        3.  Use the above model to make a prediction on all null Neighbourhood fields.
            Merge the results with given Neighbourhood data, to build complete dataset

        4.  Find entries not already in BigQuery, where Airbnb_ListingID does not exist
            Insert these rows into the table
        """

        logger.info("Matching to Neighbourhoods using ML")
    
        ## 1. Get all neighbourhood data from BigQuery
        self.clean_df = self.getCleanDataFrame()
        if self.clean_df is None:  ## Exit if no clean dataframe to work with
            return

        ## 2. Create the ML model
        self.knn_gscv = self.createKnnModel()

        ## 3. Apply the model to fill in null data fields
        self.prediction_df = self.predictWithKnnModel()

        ## 4. Push prediction to BigQuery
        self.pushPredictionToBigQuery()
        
    def getCleanDataFrame(self):
        """
        Retrieves a cleaned neighbourhood DataFrame for the current location.
        Returns:
            pandas.DataFrame or None: 
                - DataFrame containing the following columns:
                    - Airbnb_ListingID (str/int): Unique identifier for the Airbnb listing.
                    - LocalizedNeighbourhood_clean (str): Cleaned neighbourhood name.
                    - lat (float): Latitude of the listing.
                    - lng (float): Longitude of the listing.
                - Returns None if there are no listings with a non-null `LocalizedNeighbourhood_clean`.
        """

        df = self.ctx.gcp_manager.runQueryToDataFrame(
            f"""SELECT Airbnb_ListingID, LocalizedNeighbourhood_clean, lat, lng,
            FROM `{GCP_BIGQUERY_TABLES['neighbourhoodCleanInput']}`
            WHERE location = '{self.ctx.location}' AND country = '{self.ctx.country}'""" )

        logger.debug(f"Initial Dataframe shape: {df.shape}")

        if len(df[df['LocalizedNeighbourhood_clean'].notna()]) == 0:
            logger.info("Zero given neighoburhoods, skipping")
            return None

        return df
    
    def createKnnModel(self):
        """
        Trains a K-Nearest Neighbors (KNN) classifier to predict cleaned neighbourhoods 
        based on listing latitude and longitude.

        This method performs the following steps:
        1. Filters the DataFrame to include only listings with non-null `LocalizedNeighbourhood_clean`.
        2. Encodes the `LocalizedNeighbourhood_clean` categorical labels into integers.
        3. Uses latitude (`lat`) and longitude (`lng`) as features (X) and the encoded 
        neighbourhoods as labels (y).
        4. Splits the data into training (80%) and testing (20%) sets.
        5. Performs grid search cross-validation to find the optimal number of neighbors 
        (`n_neighbors`) from 1 to 24.
        6. Fits the KNN classifier to the training data.
        7. Evaluates accuracy on the test set and logs both the best CV score and test accuracy.

        Returns:
            GridSearchCV: The trained KNN model after grid search, including the best 
            `n_neighbors` parameter.

        Logs:
            - Mean cross-validation score for the optimal number of neighbors.
            - Accuracy score on the test set.
        """

        df = self.clean_df

        ## Model_df is based on filled in Neighbourhood valus
        model_df = df[df['LocalizedNeighbourhood_clean'].notna()]
        
        ## Encode categorical field into integers for modelling
        neighbourhood_encoded = le.fit_transform(model_df['LocalizedNeighbourhood_clean'])
        
        ## Use X as the predicting Latitude and Longitude
        X = model_df.loc[:, ['lat', 'lng']]
        y = neighbourhood_encoded
        
        ## Split dataset into train and test
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=17)
        
        ## Create knn classifier
        knn = KNeighborsClassifier()
        
        ## Create dictionary of all n_neighbours values we want to search for
        param_grid = {'n_neighbors': np.arange(1, 25)}
        
        ## Use gridsearch to test all n values from param_grid
        knn_gscv = GridSearchCV(knn, param_grid, cv=5)
        
        ## Fit model to data
        knn_gscv.fit(X_train, y_train)
        
        ## Make a prediction for unseen data
        y_pred = knn_gscv.predict(X_test)
        
        logger.info(f"Mean score for ideal k neighbours value: {knn_gscv.best_score_}")
        logger.info(f"Accuracy score on unseen data: {metrics.accuracy_score(y_test, y_pred)}")
        
        return knn_gscv
    
    def predictWithKnnModel(self):
        """
        Uses the trained KNN model to predict missing cleaned neighbourhoods for listings.

        This method performs the following steps:
        1. Identifies rows in `self.clean_df` where `LocalizedNeighbourhood_clean` is NaN.
        2. Extracts latitude (`lat`) and longitude (`lng`) for these listings.
        3. Uses the trained KNN model (`self.knn_gscv`) to predict the corresponding neighbourhoods.
        4. Converts the predicted integer labels back to their original categorical values using 
        the label encoder (`le`).
        5. Merges the predicted neighbourhoods back into the original DataFrame, filling in 
        missing values while preserving existing ones.
        6. Cleans up temporary columns used during prediction.

        Returns:
            pandas.DataFrame: The original DataFrame with missing `LocalizedNeighbourhood_clean` 
            values filled using KNN predictions. Columns include all original columns.

        The returned DataFrame overwrites NaN neighbourhoods but retains original non-NaN values.
        """

        df = self.clean_df 
        knn_gscv  = self.knn_gscv
        
        ## Isolate Latitude and Longitude columns where Neighbourhood == NaN
        nan_df = df[df['LocalizedNeighbourhood_clean'].isna()]
        nan_df_latlng = nan_df.loc[:, ['lat', 'lng']]

        ## Predict Neighbourhood values using Model and Lat/Lng values
        nan_predict = knn_gscv.predict(nan_df_latlng)
        
        ## Transform prediction back to label
        nan_predict = le.inverse_transform(nan_predict)
        
        ## Remove unnecessary columns before merge
        nan_df = nan_df.drop(['lat', 'lng', 'LocalizedNeighbourhood_clean'], axis=1)
        nan_df['localizedNeighbourhood_predicted'] = nan_predict

        ## Merge predictions into Neighbourhood column for each unique Airbnb_ListingID
        merged_df = pd.merge(df, nan_df, how='left', left_on=['Airbnb_ListingID'], right_on=['Airbnb_ListingID'])
        merged_df['LocalizedNeighbourhood_clean'] = merged_df['localizedNeighbourhood_predicted'].fillna(merged_df['LocalizedNeighbourhood_clean'])

        ## Drop the temporary columns used for merging
        merged_df = merged_df.drop(['localizedNeighbourhood_predicted'], axis=1)
        
        return merged_df
    
    def getCurrentMLNeighbourhoodIDs_FromBigQuery(self):
        """
        Returns a df with one column, Airbnb_ListingID, for all previously saved 
        identifiers in that location
        """
        df = self.ctx.gcp_manager.runQueryToDataFrame( f""" SELECT Airbnb_ListingID 
            FROM `{GCP_BIGQUERY_TABLES['neighbourhoodPredicted']}`
            WHERE location = '{self.ctx.location}' AND country = '{self.ctx.country}' """ )
            
        return df
    
    def pushPredictionToBigQuery(self):
        """
        Pushes new predicted neighbourhood data to the BigQuery `neighbourhoodPredicted` table.

        This method performs the following steps:
        1. Loads the cleaned DataFrame with predicted neighbourhoods (`self.clean_df`).
        2. Retrieves the list of Airbnb_ListingIDs already present in BigQuery to avoid duplicates.
        3. Adds location and country information to the DataFrame.
        4. Filters out listings that are already present in BigQuery.
        5. Pushes only the new predictions to the `neighbourhoodPredicted` table using 
        `self.ctx.gcp_manager.PushDataFrameToBigQuery`.

        Logs:
            - Number of new predictions being pushed
        """
        ## Load in the original data frame and a dataframe of
        ## Airbnb_ListingIDs already in BigQuery, to avoid duplication
        df = self.clean_df
        current_df = self.getCurrentMLNeighbourhoodIDs_FromBigQuery()
        
        ## Add additional identifying fields to the dataframe
        df['Location'] = self.ctx.location
        df['Country'] = self.ctx.country
        
        ## Only get new Airbnb_ListingIDs, which are not already in BigQuery table
        df_filtered = df[~df['Airbnb_ListingID'].isin(current_df['Airbnb_ListingID'])]
        
        ## Push new predictions to BigQuery
        logger.info(f'Pushing {df_filtered.shape[0]} predictions to table: neighbourhoodPredicted')
        self.ctx.gcp_manager.PushDataFrameToBigQuery(df_filtered, 'neighbourhoodPredicted')
 
if __name__ == '__main__':
    print("This is the ML handler. Run web_scraper.py instead")
