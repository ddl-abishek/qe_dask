The project workflow is as follows:-

1. Download the dataset from S3 bucket and unzip it to extract all the csvs
wget https://dsp-workflow.s3.us-west-2.amazonaws.com/nyc-parking-tickets.zip

2. Run the pre-process scripts to read the csvs as dask dataframes and write cleaned data versions of the csvs in .h5 file format. A dask performance report will also be generated as an artifact. 

3. Generate the seasonality of parkiing tickets plot(.png file) for all the csvs and generate a performance reports.

4. Train the KMeans model on the dask cluster on all the csvs ans generate a performance report. 