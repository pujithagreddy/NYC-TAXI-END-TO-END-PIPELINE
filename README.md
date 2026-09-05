# NYC-TAXI-END-TO-END-PIPELINE
End-to-end NYC taxi data pipeline built with Databricks, PySpark, and Delta Lake, featuring data ingestion, transformation, quality validation, and batch orchestration.


## 1. Project Overview
This project demonstrates an end-to-end batch data pipeline built using Databricks and PySpark. The pipeline integrates NYC Yellow Taxi trip data with a taxi zone lookup dataset, transforms and enriches the data, performs data quality validation, and produces a final master Delta table that can be consumed by downstream teams.
The pipeline uses two external data sources:
1.	**NYC Yellow Taxi Trip Data**: a parquet file containing trip-level information such as pickup and drop-off timestamps, trip distance, passenger count, location IDs, fare amounts, and other trip attributes. The data is sourced from the NYC Taxi & Limousine Commission’s publicly available dataset hosted on CloudFront.
2.	**Taxi Zone Lookup Data**: a CSV dataset containing information about NYC taxi zones, including `LocationID`, borough, zone name, and service zone. The dataset is obtained from a publicly available GitHub repository.

The pipeline first downloads each source dataset and loads it into a Spark DataFrame. The datasets are then stored as managed Delta tables in Databricks:
1.	`demodb.trip_data`
2.	`demodb.taxi_zones`

A final master job reads these Delta tables and joins the taxi trip data with the zone lookup data to add pickup and drop-off location details. Additional transformations calculate metrics such as trip duration, average speed, and fare per mile. The pipeline also derives useful attributes such as trip distance category, pickup date, and pickup hour. 

Data quality checks are performed on the resulting dataset to identify issues such as missing timestamps, invalid trip distances or durations, suspicious dates, negative transaction amounts, invalid passenger counts, and unrealistic speed or fare-per-mile values. Records are assigned a `Valid` or `Invalid` data quality flag along with the corresponding reason for invalid records.

The final processed dataset is stored as the `demodb.nyc_taxi_master` Delta table. This master table provides a consolidated and validated view of the taxi trip data and can serve as a downstream data source for analytics, reporting, or further processing.

## 2. Architecture

The project follows a batch-oriented data pipeline architecture implemented in Databricks. Two independent external datasets are ingested into Databricks and stored as Delta tables. A downstream master processing stage then combines the datasets, enriches the trip data, performs transformations and data quality validation, and writes the final consolidated datasheet to a Delta table.

### Pipeline Architecture

The overall architecture can be represented as: 
<img width="1107" height="595" alt="Architecture" src="https://github.com/user-attachments/assets/cd2b5d0f-3965-4118-b6ba-74d786509b45" />
The pipeline uses Delta tables as the intermediate and final storage layer. This allows the master job to work with the ingested datasets independently of their original external file locations.

### Pipeline Layers
1. **Source Layer**  
The pipeline retrieves data from two publicly available external sources: the NYC Yellow Taxi trip dataset and the NYC Taxi Zone Lookup dataset.
2. **Processing and Storage Layer**  
Each source is ingested through a dedicated Databricks notebook and stored as a Delta table. These tables provide the datasets required by the master processing stage.
3. **Master Data Layer**  
The `master_job` reads both the Delta tables, joins the datasets using taxi location IDs, derives additional attributes, and performs data quality validation. The resulting consolidated dataset is stored in `demodb.nyc_taxi_master`.

## 3. Technologies Used

The pipeline was developed and executed using a combination of data engineering technologies for data ingestion, distributed processing, storage, transformation, and validation.

### Development and Processing Environment
1. **Databricks**  
Databricks provides the development and execution environment for the project. The notebooks were developed in Databricks and configured as tasks within a batch pipeline.
2. **Apache Spark and Pyspark**  
Apache Spark serves as the distributed processing engine, while PySpark provides the Python interface used for working with Spark DataFrames. PySpark is used for reading data, performing joins, applying transformations, creating derived columns, and implementing data quality checks.
3. **Python**  
Python is used alongside PySpark for supporting the ingestion process. The urllib.request module is used to download the sources datasets from their public URLs.

### Storage and Data Formats
1. **Delta Lake**  
Delta Lake is used as the storage layer for the pipeline. The ingested datasets ate stored as managed Delta tables in Databricks, which are then consumed by the master processing job. The final processed dataset is also stored as a Delta table.
2. **Parquet**  
Parquet is the source format of the NYC Yellow Taxi trip dataset. It provides a columnar format suitable for analytical processing with Spark.
3. **CSV**  
CSV is the source format of the Taxi Zone Lookup dataset. The file is read into a Spark DataFrame with headers and inferred data types before being stored as a Delta table.

### Querying and Data Management
1. **SQL**  
SQL is used for database and table management within Databricks. It is also used to query the stored datasets, verify the final output, and generate data quality summaries such as valid/invalid record counts and the frequency of quality issues.

## 4. Data Sources

The pipeline uses two publicly available datasets related to New York City taxi services. The datasets provide complementary information. The trip dataset contains individual taxi trip records, while the zone lookup dataset provides geographic information associated with taxi location IDs.

### NYC Yellow Taxi Trip Data
The primary dataset used in the project is the NYC Yellow Taxi Trip Data for January 2024. It is published by the New York City Taxi & Limousine Commission (NYC TLC) and accessed through an AWS CloudFront-hosted URL. The dataset is provided in Parquet format and contains trip-level information for yellow taxi journeys.

Key fields used in the pipeline include:  
i.	`tpep_pickup_datetime`: date and time when the trip started  
ii.	`tpep_dropoff_datetime`: date and time when the trip ended  
iii.	`passenger_count`: number of passengers  
iv.	`trip_distance`: distance travelled during the trip  
v.	`PULocationID`: Taxi Zone ID where the trip started  
vi.	`DOLocationID`: Taxi Zone ID where the trip ended  
vii.	`fare_amount`: base fare amount  
viii.	`total_amount`: total amount charged for the trip  

The dataset is downloaded during pipeline execution and read into a Spark DataFrame and stored in Databricks as the managed Delta table `demodb.trip_data`.  

### NYC Taxi Zone Lookup Data

The second dataset is in the NYC Taxi Zone Lookup dataset. It is available as a CSV file through a public GitHub Repository. This dataset provides descriptive information for the location IDs present in the trip dataset.

Key fields used in the pipeline include:

i.	`LocationID`: unique identifier for a taxi zone
ii.	`Borough`: New York City borough containing the zone
iii.	`Zone`: name of the taxi zone
iv.	`Service_zone`: taxi service zone classification

The dataset is downloaded during pipeline execution and read into a Spark DataFrame and stored in Databricks as the managed Delta table `demodb.taxi_zones`.

The `PULocationID` and `DOLocationID` fields from the trip dataset are matched with the `LocationID` field from the zone lookup dataset. This relationship allows the pipeline to enrich each trip with pickup and dropoff borough, zone, and service-zone information.

## 5. Data Processing, Transformations and Quality

The master processing stage combines the ingested trip and taxi zone datasets, enriches the trip records with geographical information, derives analytical metrics, and applies data quality validation rules. The processed records are retained and classified as either valid or invalid based on the defined quality checks.

### Data Integration

The master processing job first reads the two previously created Delta tables. The Taxi Zone dataset is joined with the Trip Data using the Location ID fields. A left join is performed between `PULocationID` from the Trip Data and `LocationID` from the zone lookup table to obtain pickup location details. A second left join is performed using `DOLocationID` to obtain dropoff location details.  

The resulting dataset is enriched with:  
i.	Pickup borough  
ii.	Pickup zone  
iii.	Pickup service zone  
iv.	Dropoff borough  
v.	Dropoff zone  
vi.	Dropoff service zone  

This allows the final datasets to contain both the original trip information and descriptive geographical information for each trip.

### Data Transformations  

Several derived columns are created to make the dataset more useful for analysis. 
1. **Trip Duration**  
Trip Duration is calculated using the difference between the pickup and dropoff timestamps and expressed in minutes.  
2. **Average Speed**  
Average Speed is calculated using the trip distance and trip duration. The calculation is performed only when the trip duration is greater than zero to avoid invalid division.  
3. **Fare Per Mile**  
The fare efficiency of each trip is calculated using the total trip amount divided by the trip distance. The calculation is performed only when the trip distance is greater than zero.  
4. **Trip Distance Category**  
Trips are categorized based on their recorded distance. (in miles)  
If the distance is less than or equal to zero then invalid, else if less than or equal to 1 then short, else if less than or equal to 5 then medium, else if less than or equal to 10 then long, else very long.   
5. **Date And Time Attributes**  
Additional temporal columns such as  pickup_date, pickup_hour are extracted from the pickup timestamp. These columns make the final dataset more convenient for time-based analysis.

### Data Quality Validation
Data quality rules are applied to identify potentially invalid or suspicious records. The pipeline checks for conditions including:  

i.	Missing pickup or dropoff timestamp: missing timestamp  
ii.	Pickup year earlier than 2023: suspicious pickup date  
iii.	Trip distance <= 0: invalid trip distance  
iv.	Trip duration <= 0: invalid trip duration  
v.	Trip duration > 180 minutes: excessive trip duration  
vi.	Total amount < 0: negative total amount  
vii.	Passenger count <= 0: invalid passenger count  
viii.	Average speed > 100 mph: unrealistic speed  
ix.	Fare per mile > 100: unrealistic fare per mile  

The corresponding issues are stored in the `data_quality_reason` column. A `data_quality_flag` column is then generated which shows a `Valid` if no quality issues were identified or an `Invalid` if one or more quality issues were identified.

Importantly, the pipeline doesn’t delete invalid records. Instead, the records are retained and flagged. This preserves the original data while making potentially problematic records identifiable for further investigation.

### Final Data Preparation

After the joins, transformations, and quality validation steps are completed, the resulting DataFrame represents the final enriched taxi dataset. The final dataset contains:

i.	Original trip attributes  
ii.	Pickup and dropoff geographical information  
iii.	Trip duration and average speed  
iv.	Fare per mile  
v.	Trip distance category  
vi.	Pickup date and hour  
vii.	Data quality reasons   
viii.	Data quality status  

The processed DataFrame is then written into a managed Delta table `demodb.nyc_taxi_master`, which serves as the final output of the pipeline and can be used for downstream analysis and reporting.

## 6. Pipeline Execution

The pipeline was configured and executed as a batch workflow in Databricks. The workflow consists of three notebook tasks executed in the order: `trip_data_job`, `taxi_zone_job`, `master_job`. 

<img width="841" height="160" alt="image" src="https://github.com/user-attachments/assets/cf0cdaa3-7cae-4f87-995b-d8b56d0f0c05" />

During execution, the ingestion tasks download the required source datasets and store them as managed Delta tables in Databricks. The `master_job` then reads these tables, performs the required joins, transformations, and data quality validation, and writes the processed data to the final table. 

The final output of the pipeline is stored in `demodb.nyc_taxi_master`. The pipeline uses overwrite mode when writing the tables, allowing each execution to refresh the datasets produced by the corresponding tasks. 

## 7. Output and Results

After the pipeline completed, the execution status of each task was checked to ensure that the workflow ran successfully. The final output was then verified by querying `demodb.nyc_taxi_master`. The verification included:

i.	Checking the final table schema  
ii.	Inspecting sample records  
iii.	Checking valid and invalid record counts  
iv.	Reviewing data quality reasons for invalid reasons  
v.	Checking maximum trip duration, average speed, and fare per mile.   

A successful pipeline execution confirms that the workflow completed without execution errors and that the final table was successfully created or refreshed. 

Data validity is evaluated separately through the data quality checks implemented in the master processing stage. Therefore, a successful pipeline run does not imply that every record is valid.

The resulting table provides a single, analysis-ready view of the taxi trip data by combining trip-level information with geographical attributes and derived analytical metrics. Data quality issues are preserved and explicitly flagged rather than being removed, allowing downstream users to identify and investigate potentially problematic records. 

The final dataset can therefore serve as a foundation for further analysis such as:  
i.	Trip volume analysis by borough or taxi zone  
ii.	Distance and duration analysis  
iii.	Fare and pricing analysis  
iv.	Hourly or daily trip patterns  
v.	Identification of unusual or potentially anomalous trips

## 8. Conclusion and Future Improvement

### Conclusion

This project demonstrates the development of an end-to-end batch data pipeline using Databricks and PySpark. Two publicly available NYC taxi datasets were ingested, stored as managed Delta tables, integrated through joins, and transformed into a single enriched master dataset. 
The pipeline also incorporates data quality validation by identifying and flagging records with missing, invalid, or suspicious values while retaining the original records for further analysis. 
The final output, `demodb.nyc_taxi_master`, provides a structured and analysis-ready dataset containing trip information, geographical attributes, derived metrics, and data quality indicators. 
Overall, the project demonstrates key data engineering concepts including data ingestion, distributed processing, data transformation, table storage, data quality validation, and batch pipeline orchestration.

### Future Improvements

The current pipeline can be further enhanced to make it more scalable, reusable and closer to a production-ready data engineering solution. 

i.	**Incremental Data Processing**  
Instead of processing the entire dataset each time, the pipeline could be modified to process only newly available data, reducing processing time and resource usage.  
ii.	**Performance Optimization**  
The pipeline could be optimized using techniques such as appropriate partitioning, caching where beneficial, and query optimization for larger datasets.  
iii.	**Downstream Analytics**  
The final master table could be connected to a visualisation or business intelligence tool to build dashboards showing taxi demand, trip patterns, fares, distances, and other trends.






