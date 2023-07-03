# DE_space_launch_project

Welcome to my GitHub repository for a Data Engineering project focused on analyzing space launch data. In this project, I utilize the Space Launch API along with tools like Apache Airflow, Azure Storage, Databricks, and Power BI to retrieve, transform, store, and analyze crucial information about space launches.

To ensure seamless data pipeline orchestration, I rely on Apache Airflow. With Airflow, I schedule regular data retrieval from the Space Launch API, keeping my dataset up-to-date. Additionally, Airflow allows me to streamline data transformations to align with my analysis objectives.

For efficient management of large volumes of data, I leverage Azure Storage, a secure and scalable cloud storage solution. This ensures easy access, high availability, and resilience against failures, with various storage options to suit my specific requirements.

To perform complex data processing, I harness the power of Databricks. Databricks provides a collaborative environment with Apache Spark, enabling me to efficiently perform intricate data manipulations, aggregations, and calculations at scale.

Once data transformations are complete, I utilize the robust data analysis and visualization capabilities of Power BI. With Power BI, I gain valuable insights into the success rates and cost trends of space launches through compelling visualizations and reports.


## Overall Architecture:
![alt text](https://github.com/LucaLiverani/DE_space_launch_project/blob/main/DE_space_launch_project_image/Architecture.png?raw=true)
## Data Model
![alt text](https://github.com/LucaLiverani/DE_space_launch_project/blob/main/DE_space_launch_project_image/Star_schema.png?raw=true)
## ETL from the API to Databricks
![alt text](https://github.com/LucaLiverani/DE_space_launch_project/blob/main/DE_space_launch_project_image/ETL_airflow.png?raw=true)
