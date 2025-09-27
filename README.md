# Stock-Market-Data-Pipeline-AWS-Kafka
Real-time stock market analytics pipeline using AWS

This project demonstrates a real-time stock market analytics pipeline using Apache Kafka, AWS Glue, Amazon S3, and Amazon Athena, with dashboard.

Even though the dataset is simulated, the architecture is production-ready and can handle real-time data streams with minimal changes.

## Tech Stack

- **Streaming**: Apache Kafka (EC2/Docker), Python (producer & consumer scripts)  
- **Storage**: Amazon S3 (Raw Zone & Curated Zone)  
- **ETL / Metadata**: AWS Glue (ETL jobs, Crawlers, Data Catalog)  
- **Analytics**: Amazon Athena (serverless SQL queries)  
- **Visualization**: Amazon QuickSight (or Tableau / Power BI) 


## Project Structure

stock-market-analytics-aws/

│── README.md                # Project documentation

│── architecture.png          # Architecture diagram

│── data/

│   └── stocks.csv           # Sample dataset

│── kafka/

│   ├── producer.py          # Kafka producer script

│   └── consumer.py          # Kafka consumer script

│── glue/

│   └── glue_elt_stock.py    # Glue ETL script (clean/transform/load)

│── athena/

│   ├── top_gainers.sql      # Athena SQL for top gainers

│   └── volatility.sql       # Athena SQL for volatility

│── notebooks/               # Jupyter notebooks for analysis

│── docs/

│   ├── athena_query.png     # Athena query example

│   └── dashboard.png        # Dashboard screenshot


Architecture

![Architecture](./Stock%20market%20project%20architecture.png)


Flow:

Data Source

Stock CSV dataset (simulated with a Python script).

Data Streaming

Producer (Python app) pushes stock records into Kafka (EC2/Docker).

Consumer (Python app) reads from Kafka and writes records into Amazon S3 (Raw Zone).

ETL (Extract, Transform, Load)

AWS Glue ETL job (glue_elt_stock.py) cleans, transforms, and optimizes the raw data.

Writes curated data to Amazon S3 (Curated Zone) in Parquet format, partitioned by date/ticker.

Metadata Management

AWS Glue Crawler discovers schema from raw/curated data.

Glue Data Catalog stores table definitions for Athena.

Analytics

Amazon Athena queries curated data directly from S3 using SQL.

Visualization

Dashboards created using Amazon QuickSight (or external BI tools such as Tableau/Power BI).






