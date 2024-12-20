## BEES Data Engineering – Breweries Case

This repository contains the code and configuration of a project for a data pipeline that fetch and processes brewery data from the Open Brewery DB API and persists it into a data lake following the medallion architecture.

### Project Overview

This project demonstrates skills in:

* **Data Integration**: Retrieving and integrating data from the Open Brewery DB API
* **Data Processing**: Transforming, cleaning, and processing large datasets efficiently using PySpark
* **Pipeline Orchestration**: Automating and scheduling data workflows with Apache Airflow, including retries and error handling mechanisms
* **Medallion Architecture**: Implementing a structured data lake architecture with bronze, silver, and gold layers for better data organization and quality
* **Containerization**: Ensuring consistent and portable environments using Docker and Docker Compose
* **Code Reusability**: Developing modular and reusable code components to simplify maintenance and scalability
* **Dependency Injection and Inversion of Control**: Decoupling components to enhance flexibility, testability, and maintainability
* **Configuration Management**: Centralizing configurations to make the pipeline adaptable to different environments (local or cloud)
* **Monitoring and Logging**: Implementing comprehensive logging and monitoring to ensure data quality and pipeline reliability
* **Partitioning and Optimization**: Using partitioning strategies and columnar storage formats like Parquet to improve query performance and efficiency
* **Flexible Data Lake Storage**: Supporting configurable storage options to switch seamlessly between a local file system and AWS S3 for the data lake, allowing adaptability based on project requirements.

### Technologies

* **Programming Language**: Python
* **Data Processing**: PySpark
* **Orchestration**: Apache Airflow
* **Containerization**: Docker, Docker Compose


### Technical and design decisions

#### Data Profiling
* Profiling: Before applying transformations, the data was evaluated using simple Python and PySpark scripts executed locally. Key insights from analyzing the bronze layer data guided the development strategy:
    1. The `country` field has inconsistencies with leading and/or trailing spaces in the names, making trimming necessary for data consistency.
    2. Fields like `name`, `city`, and `state` contain special characters, necessitating translation and replacement.
    3. Only the `ID`, `name`, `brewery_type`, `city`, `country`, and `state` columns are fully populated; the other columns may contain null values.
    4. The values in the `address_1` and `street` fields, as well as in `state` and `state_province`, were often identical, indicating redundancy that may need to be addressed.

#### Bronze Layer - Data Acquisition
* Data Format: Using Parquet for the bronze layer ensures efficient storage, faster reads, and compatibility with Big Data tools like Apache Spark, making it ideal for handling large datasets.
* Requests library: The Requests library is a simple, user-friendly, and native Python tool, making it the ideal choice for interacting with open APIs.
* Config file: Uses a config file to ensure centralized management of settings, promoting flexibility, reusability, and easier maintenance across the project.

#### Silver Layer - Data Transformations
* Data cleaning and processing: Performing data cleaning in the silver layer ensures high data quality and consistency, making the data reliable for analysis.
* Partitioning: Organizing data by `country` and `state` in the silver layer improves query efficiency in the gold layer, enabling faster retrieval based on location. Further partitioning by attributes like `brewery_type` can be added based on specific analytical requirements. Partitioning by `city` was intentionally avoided to prevent generating a large number of small files and excessive partitions, which could negatively impact performance and resource management.

#### Gold Layer - Data Aggregation
* Aggregation Strategy: Generating an aggregated view with the `brewery count` by `type` and location, partitioned by `country`, provides a pre-computed dataset that enables efficient analysis. Additional aggregation levels, such as by `state` or `city`, can be considered to meet specific analytical needs and enhance data exploration capabilities.
* Denormalization: The gold layer utilizes a denormalized structure that includes `brewery type`, `location`, and `brewery count`. This approach minimizes the need for complex joins, thereby enhancing query performance. Depending on future requirements, further denormalization or adopting a Star Schema design, as recommended by Kimball’s methodology, could be evaluated to balance performance and query complexity.

### Storage Architecture

The storage adheres to the medallion architecture:

* **Bronze Layer:** Stores raw data extracted from the API in Parquet format within the bronze directory. This layer maintains the data in its original form for reference and auditing purposes.
* **Silver Layer:** Converts the data to Apache Parquet format for efficient storage, applying transformations, cleansing, and partitioning by country and state within the silver directory. Schema validation and inconsistency corrections are also performed at this stage.
* **Gold Layer:** Creates an aggregated view with brewery counts by type and location, partitioned by country, within the gold directory. This layer provides pre-aggregated data optimized for quick and efficient analysis.

### Project Structure

```
Breweries_Case-Data_Engineering/
├── dags/
│   ├── bronze_fetch_data_from_api.py        # DAG for data ingestion into the bronze layer
│   ├── silver_transform_data_from_bronze.py # DAG for data transformation into the silver layer
│   └── gold_aggregate_data_from_silver.py   # DAG for data aggregation into the gold layer
│
├── include/
│   ├── bronze_fetch_data_from_api/
│   │   └── tasks.py                         # Tasks specific to the bronze layer
│   ├── silver_transform_data_from_bronze/
│   │   └── tasks.py                         # Tasks specific to the silver layer
│   ├── gold_aggregate_data_from_silver/
│   │   └── tasks.py                         # Tasks specific to the gold layer
│   └── datalake/
│       ├── base_datalake.py                 # Base class for the Data Lake
│       ├── local_datalake.py                # Implementation for local Data Lake storage
│       ├── s3_datalake.py                   # Implementation for AWS S3 Data Lake storage
│       └── datalake_factory.py              # Factory to manage different Data Lake implementations
│
├── data/
│   ├── bronze/                              # Raw data stored in Parquet format
│   ├── silver/                              # Transformed data in Parquet format, partitioned by country and state
│   └── gold/                                # Aggregated data ready for analysis
│
├── logs/                                    # Logs generated by Airflow
│
├── plugins/                                 # Additional plugins for Airflow
│
├── tests/
│   ├── test_bronze_fetch_data_breweries_api.py     # Tests for the bronze layer data ingestion
│   ├── test_silver_transform_data_from_bronze.py   # Tests for the silver layer data transformation
│   └── test_gold_aggregate_data_from_silver.py     # Tests for the gold layer data aggregation
│
├── config/
│   └── config.py                            # Centralized configuration file for the project
│
├── docker-compose.yml                       # Docker Compose file for running all containers
├── docker-compose.override.yml              # Override file for Docker Compose configurations
├── Dockerfile                               # Dockerfile to extend the Airflow image with additional setup
├── .env                                     # Environment variables file for project configuration
│
├── README.md                                # Main documentation file for the project


```

### Project Overview

![image](/doc_images/project-overview.svg)

1. Apache Airflow triggers the ["bronze_fetch_data_breweries_api"](/dags/bronze_fetch_data_breweries_api.py) DAG, which handles data extraction from the Open Brewery DB API. The process begins by accessing the metadata endpoint https://api.openbrewerydb.org/v1/breweries/meta. This step is crucial because the metadata reveals the total number of available records, serving as a reference for quality checks — the number of files should match the total objects divided by the requested per_page limit. The DAG then fetches the actual data by calling the https://api.openbrewerydb.org/v1/breweries endpoint in a loop, using the per_page=200 parameter to retrieve the data in batches and persist to the bronze layer in parquet format.
2. In the same DAG, there is a component dedicated to storing each page of data in the bronze layer. The data chunks are saved as Parquet files within a folder named `brewery_data_{execution_date} `(e.g., `brewery_data_2024-05-31`). The naming convention currently uses only the date since the DAG is scheduled to run daily. However, this can be adjusted to include hours and minutes (e.g., `brewery_data_2024-05-31-17-36`) based on evolving business requirements. Upon completing this task, the DAG triggers the next step in the pipeline, ensuring seamless progression to the subsequent DAG for further processing.
3. The next step in the pipeline is the ["silver_transform_data_from_bronze"](/dags/silver_transform_data_from_bronze.py) DAG. This DAG is responsible for loading the specific date folder from the bronze layer into a Spark DataFrame, followed by data cleansing, transformation, and formatting. The key transformations performed include:
    * Column Selection: Only relevant columns are retained in the dataset to reduce unnecessary data load.
    * Trimming and Character Replacement: Issues with trailing spaces, non-UTF-8 characters, and special characters are addressed in fields like country, state, and name. For instance: The state of Kärnten, where the `ä` character would appear as `k�rnten`.
        * The state of Kärnten, where the `ä` character would appear as `k�rnten`, the behaviour is also observed in the [API Website itself](https://www.openbrewerydb.org/breweries/Austria/K%EF%BF%BDrnten);
        * Edge Case Handling: Additional data anomalies were identified and resolved
    * General Character Normalization: The unidecode package is used to replace special characters and remove accents to ensure consistency.
After applying these transformations, the DAG saves the DataFrame to the silver layer in Parquet format, partitioned by `country` and `state` to optimize query performance.
* All DAGs are configured to automatically retry up to three times with a 5-minute interval between each attempt in case of failure.

4. The final step is handled by the ["gold_aggregate_data_from_silver"](dags/gold_aggregate_data_from_silver.py) DAG. This DAG begins by retrieving the latest data folder from the silver layer and utilizes Spark to perform aggregations. Specifically, it groups the data by `brewery_type`, `country`, and `state`, and calculates the total number of breweries in each group. The resulting aggregated dataset is stored in the gold layer in Parquet format, partitioned by `country`, ensuring efficient retrieval for analytical purposes.

**Code comments are included throughout the DAGs to clarify the data transformations and PySpark operations**

### Running the Pipeline

**Prerequisites:**

1. Docker and Docker Compose installed
    * You can get Docker in your computer by following the instructions on ["docs.docker"](https://docs.docker.com/get-docker/)
2. Terminal of your preference, this project was built using Bash in Linux Ubuntu
3. Optional- Configuring the Data Lake on AWS S3: If you choose to use AWS S3 as your data lake, you need to update the configuration in the .env file. Follow the official AWS tutorial to create and set up your S3 bucket: [Creating an S3 Bucket](https://docs.aws.amazon.com/AmazonS3/latest/userguide/creating-buckets-s3.html).

### Points of Attention

* If you are using macOS with an ARM64 (Apple Silicon) architecture, you need to adjust the Java installation path in the Dockerfile. Modify line 14 to update the JAVA_HOME environment variable:

* Original (for Linux and Intel systems): `ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64/`
* Modified (for macOS ARM64): `ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-arm64/`

This change ensures the correct Java path is set in Docker for machines using Apple M1/M2 processors.

* In the .env file, the default value of AIRFLOW_UID=50000 may not have the necessary permissions on some Linux systems. To resolve this:

1. Find your current user UID by running the following command in the terminal:
 $ `id -u`


**Steps:**

1. Clone this repository, you can:
    * Download as ZIP folder directly from GitHub;
    * Use the Git CLI to clone this repository using `git clone https://github.com/JStCarlos/Breweries_Case-Data_Engineering.git`;
2. To build and launch the Docker containers, run the following commands in your terminal within the project folder:
    * Initialize Airflow configuration by executing:

     $ `docker-compose up airflow-init`

    * Build and start the containers with:
    
     $ `docker-compose up --build`

    All project services will run in separate Docker containers, ensuring modularity and ease of maintenance. When you start the containers using Docker Compose, each service (such as Airflow, Spark, Redis, and PostgreSQL) will be deployed independently.

    The configurations, including connections to services like Spark, are automatically managed within the Docker network. This means no manual intervention is needed to establish communication between containers — for example, Airflow will automatically connect to the `spark-master` container for processing tasks using the Airflow CLI and scripting.

    This approach ensures a consistent and reliable environment, making it easier to deploy, scale, and manage the data pipeline.

3. Open the Airflow web interface (usually available at `http://localhost:8080`) and manually trigger the ["bronze_fetch_data_breweries_api"](dags/bronze_fetch_data_breweries_api.py) DAG to start the data ingestion process.
4. Wait for the execution of all DAGs to finish. The pipeline consists of the following three DAGs:
    * bronze_fetch_data_from_api – Ingests raw data from the API into the bronze layer.
    * silver_transform_data_from_bronze – Cleans and transforms the data into the silver layer.
    * gold_aggregate_data_from_silver – Aggregates the data and stores it in the gold layer for analysis.
5. Once all the DAGs have successfully finished, you can check the directory ["./Breweries_Case-Data_Engineering/data"](data/) on your local machine to view the files stored in each layer (bronze, silver, and gold).
6. Finally, you can hit your terminal with `docker-compose down` and stop the containers.

### Monitoring and Alerting

In this project, the Airflow webserver serves as the primary interface for tracking the success or failure of DAG executions. Although a full monitoring and alerting system has not been implemented, setting up such a system is essential for production environments. Here are some approaches to consider:

* Airflow Built-in Monitoring: Utilize Airflow's native features to monitor DAG runs, detect failures, and configure           notifications through email or messaging platforms.

* Data Quality Validation: Integrate data quality checks within the pipeline to verify data integrity and consistency. Configure alerts to notify stakeholders when anomalies or quality issues are detected.

* Third-party Monitoring Tools: Incorporate external tools like Prometheus or Datadog for a more comprehensive view of system performance, infrastructure health, and to enable advanced alerting mechanisms.

These strategies help ensure that your data pipeline remains reliable, and any issues are identified and addressed promptly.