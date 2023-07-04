# DE_space_launch_project

Welcome to my GitHub repository for a Data Engineering project focused on analyzing space launch data. In this project, I utilize the Space Launch API along with tools like **Apache Airflow**, **Azure Storage**, **Databricks**, and **Power BI** to retrieve, transform, store, and analyze crucial information about space launches.

To ensure seamless data pipeline orchestration, I rely on Apache Airflow. With Airflow, I schedule regular data retrieval from the Space Launch API, keeping my dataset up-to-date.

For efficient management of large volumes of data, I leverage Azure Storage, a secure and scalable cloud storage solution. This ensures easy access, high availability, and resilience against failures.

To perform complex data processing, I harness the power of Databricks. Databricks provides a collaborative environment with Apache Spark, enabling me to efficiently perform intricate data manipulations, aggregations, and calculations at scale.

Once data transformations are complete, I utilize the robust data analysis and visualization capabilities of Power BI. With Power BI, I gain valuable insights into the success rates and cost trends of space launches through compelling visualizations and reports.

## Overall Architecture:

![alt text](https://github.com/LucaLiverani/DE_space_launch_project/blob/main/DE_space_launch_project_image/Architecture.png?raw=true)

## Data Model

![alt text](https://github.com/LucaLiverani/DE_space_launch_project/blob/main/DE_space_launch_project_image/Star_schema.png?raw=true)

I applied **normalization techniques** to convert a single JSON file into a star schema for efficient data analysis. The JSON file contained comprehensive information about rocket launches, but it was denormalized, meaning all data was in a single structure.

To begin, I identified distinct entities within the JSON file: rockets, agencies, missions, and launch pads. I then separated the data into a fact table, "ft_launch," and four dimension tables: "dm_rocket," "dm_agency," "dm_mission," and "dm_pad."

The fact table, "ft_launch," served as the central table and contained core data points for each rocket launch. It included foreign keys to link to the dimension tables and additional measures like launch success status and payload mass.

Each dimension table provided contextual information about the entities involved in the launches. The "dm_rocket" table captured rocket-specific details, such as name, manufacturer, country, and payload capacity. The "dm_agency" table stored information about space agencies, including name, country, and establishment year. The "dm_mission" table held data about each mission, such as name, type, launch date, and destination. Lastly, the "dm_pad" table contained details about the launch pads, including name, location, and launch complex.

To establish relationships between the tables, I created foreign keys in the fact table that referenced the primary keys of the corresponding dimension tables. These foreign keys facilitated efficient querying and analysis.
## ETL from the API to Databricks

![alt text](https://github.com/LucaLiverani/DE_space_launch_project/blob/main/DE_space_launch_project_image/ETL_airflow.png?raw=true)

The **ETL process** is implemented using Airflow and Databricks. The first step involves checking the availability of the Space Launch API. Data is extracted from the API and stored as a Parquet file in Azure Storage. Then, Databricks is utilized to load the data into a staging table and execute parallel jobs for transformation and loading into dimension tables.

Once the dimension tables are loaded, the principal fact table, ft_launch, is loaded. This table consolidates key metrics and measurements related to each space launch. The loading process includes rigorous quality control checks.

To update the tables efficiently, the **MERGE** operation provided by Databricks is utilized. This operation synchronizes existing records and inserts new ones, maintaining data accuracy.

## Data Analysis on PowerBI

The project also includes a dedicated dashboard in Power BI to visualize and explore launch success and failure rates based on different attributes. The page provides interactive visualizations and filters to allow users to drill down into specific agencies, rockets, or launch pads.



Additionally, another page in Power BI is designed specifically for cost analysis per launch. The dashboard provides insights into the cost of launches by agency and rocket. It also identifies key influencers that contribute to increased costs. Moreover, the dashboard displays the cost associated with the payload mass, enabling users to understand the cost implications related to payload.

