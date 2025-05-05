# 🗽 NYC Subway Foot Traffic Prediction & Forecasting

## Table of Contents
- [Executive Summary](#executive-summary)
- [Project Structure](#project-structure)
- [Getting Started: Setup Instructions](#getting-started-setup-instructions)
  - [Prerequisites](#prerequisites)
  - [Kafka Installation](#kafka-installation)
  - [Terminal Setup](#terminal-setup-run-in-4-terminals)
  - [Apache PySpark & SparkML Setup for EDA Notebook](#apache-pyspark--sparkml-setup-for-eda-notebook)
  - [Notebook Execution Order](#notebook-execution-order)
- [EDA & Modeling](#exploratory-data-analysis--sparkml-historical-modeling-edasparkml_analysis)
- [Kafka Simulation & Streaming](#kafka-simulation--streaming-ingestion-kafka)
  - [Kafka Data Producer](#kafka-data-producer-send_turnstile_datash)
  - [Kafka Consumer](#kafka-consumer-kafka_stream_consumeripynb)
- [Model Training](#model-training-with-sparkml-models)
- [Live Prediction](#real-time-foot-traffic-forecasting-streaming)
- [Aggregated Variant](#extra-aggregated-modeling-variant-aggregate_model_v2)
- [Lessons Learned](#lessons-learned)
- [Sample Output](#sample-prediction-output)
- [Data Source](#data-source)
- [Future Improvements](#future-improvements)


## Executive Summary

This project implements a real-time big data pipeline to analyze and forecast passenger foot traffic across NYC subway stations using MTA turnstile data. By simulating and ingesting real-time data using Apache Kafka, transforming it with PySpark, and storing it in MongoDB, we built predictive models using SparkML to support smarter transit operations.

## **Tech Stack**

* **Apache Spark / PySpark** — Distributed computing and real-time stream processing (Structured Streaming & MLlib)
* **SparkML** — Machine learning pipeline for Random Forest modeling and feature engineering
* **Apache Kafka** — Real-time data ingestion and synthetic stream simulation
* **MongoDB / NoSQL** — Persistent storage for structured turnstile event data
* **Python** — Primary programming language across all modules
* **Matplotlib / Seaborn / Pandas** — Exploratory data analysis and data visualization
* **SQL / Spark SQL** — Querying and transforming streaming and batch data
* **Google Colab** — EDA, feature engineering, and model prototyping in notebooks
* **Bash** — Shell scripting for Kafka data producer automation


---

## Project Structure

```bash
nyc-subway-foot-traffic-prediction-and-forecasting/
├── aggregate_model_v2/            # (EXTRA) Aggregated foot traffic pipeline (entries + exits)
│   ├── stream_aggregated_consume_predict.ipynb
│   ├── stream_consume_aggregate.ipynb
│   └── training_model_aggregate.ipynb
├── eda_sparkml_prediction/        # EDA + historical modeling using SparkML
│   └── nyc_turnstile_eda_sparkml.ipynb
├── kafka/                         # Kafka simulation and consumer pipeline
│   ├── kafka_stream_consumer.ipynb
│   └── send_turnstile_data.sh
├── models/                        # Model training notebooks
│   └── training_model.ipynb
├── streaming/                     # Live inference on streamed data
│   └── stream_consume_predict.ipynb
├── .gitignore
├── README.md
└── requirements.txt 
```
---

## Getting Started: Setup Instructions

### Prerequisites

```bash
# Required Software
- Apache Kafka 2.13 (with Zookeeper)
- Apache Spark (with spark-sql-kafka and MongoDB connector)
- MongoDB
- Python 3.x
```

### Kafka Installation

To install Kafka 3.5.0 (Scala 2.13):

```bash
wget https://downloads.apache.org/kafka/3.5.0/kafka_2.13-3.5.0.tgz
tar -xzf kafka_2.13-3.5.0.tgz
cd kafka_2.13-3.5.0
```

### Terminal Setup (Run in 4 terminals)

```bash
# Terminal 1 — Zookeeper
cd ~/kafka_2.13-3.5.0
bin/zookeeper-server-start.sh config/zookeeper.properties

# Terminal 2 — Kafka Server
cd ~/kafka_2.13-3.5.0
bin/kafka-server-start.sh config/server.properties

# Terminal 3 — Kafka Producer (Turnstile Simulator)
chmod +x ~/send_turnstile_data.sh
~/send_turnstile_data.sh

# (Optional) Inspect the script
nano ~/send_turnstile_data.sh

# Terminal 4 — MongoDB
mongod
```

---

### Apache PySpark & SparkML Setup for EDA Notebook

If using **Google Colab**:

No local installation required. The environment supports PySpark. Use the following setup cell at the top of your notebook:

```python
!apt-get install openjdk-11-jdk-headless -qq > /dev/null
!wget -q http://apache.osuosl.org/spark/spark-3.1.2/spark-3.1.2-bin-hadoop3.2.tgz
!tar xf spark-3.1.2-bin-hadoop3.2.tgz
!pip install -q findspark

import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-3.1.2-bin-hadoop3.2"

import findspark
findspark.init()
```

If running **locally** (Jupyter or VSCode):

Install the following Python packages:

```bash
pip install pyspark pandas matplotlib seaborn numpy
```

Ensure that `JAVA_HOME` is correctly set in your system's environment variables (Java 8 or Java 11 is recommended).

---

### Notebook Execution Order

```bash
# 1. EDA and Historical Modeling
eda_sparkml_analysis/nyc_turnstile_eda_sparkml.ipynb

# 2. Kafka Stream Consumer + MongoDB Writer
kafka/kafka_stream_consumer.ipynb

# 3. Model Training Pipeline
models/training_model.ipynb

# 4. Live Prediction via Streaming Inference
streaming/stream_consume_predict.ipynb
```


---

## Exploratory Data Analysis & SparkML Historical Modeling (`eda_sparkml_analysis/`)

The exploratory data analysis (EDA) phase involved parsing, cleaning, and extracting insights from over **13 million rows** of raw NYC MTA turnstile data. The goal was to identify key trends, ridership behaviors, and temporal patterns to guide feature engineering and model design.

We began by removing duplicates and transforming the timestamp fields into a unified datetime format for each record. Using **Pandas** and **Seaborn** for initial visualizations, and then transitioning to **PySpark** for scalability, we explored hourly, daily, and seasonal ridership trends across major NYC stations.

We calculated net foot traffic using the difference between consecutive `ENTRIES` and `EXITS`, and visualized the busiest times and locations. Peak ridership was observed during weekday rush hours (7–10 AM and 5–8 PM), and major stations like **34 ST-HERALD SQ** and **TIMES SQ-42 ST** consistently ranked among the busiest in terms of both volume and frequency.

Following the exploratory insights, we implemented a **SparkML-based prediction pipeline** using Random Forest Regression. Key temporal features were extracted, such as `hour`, `day_of_week`, and a station index (generated using `StringIndexer`). These were assembled into a feature vector using `VectorAssembler` and passed into `RandomForestRegressor` models.

We trained two separate models to predict `ENTRIES` and `EXITS`. Model evaluation was based on RMSE and feature importance scores, which revealed that **station identity** and **hour of day** were the most influential predictors. The trained models were serialized as Spark `PipelineModel` objects and later reused for real-time prediction.

---

### How to Reproduce This Phase


1. Upload CSV data files to the notebook directory

2. **Run Notebook**

   * Open the `nyc_turnstile_eda_sparkml.ipynb` notebook (or `BIgdataProjectfile.ipynb` if renamed).
   * Execute all cells sequentially:

     * Data Loading & Inspection:

       * Use `pandas.read_csv()` for initial loading
       * Check data types, missing values, and descriptive stats

     * Data Cleaning & Preprocessing:

       * Drop nulls
       * Combine `C/A`, `UNIT`, `SCP` into `turnstile`
       * Merge `date` and `time` into `datetime`
       * Derive `hour`, `day_of_week`, and `FOOT_TRAFFIC`

     * Visualization (EDA):

       * Use Seaborn and Matplotlib to visualize:

         * Hourly trends
         * Weekday patterns
         * Station-level activity
         * Heatmaps and boxplots

     * Feature Engineering:

       * Convert categorical features (like `station`) using StringIndexer
       * Assemble features using VectorAssembler

     * Modeling (Spark ML):

       * Train Linear Regression, Decision Tree, and Random Forest models
       * Evaluate performance using RMSE, MAE, and R²

3. **Results**

   * Model performance metrics: **RMSE**, **MAE**, and **R²** printed per model
   * EDA results are displayed as inline charts and plots

This historical modeling step was critical in establishing a baseline understanding of foot traffic behavior and ensuring the streaming predictions had a strong statistical foundation.

---
## Kafka Simulation & Streaming Ingestion (`kafka/`)

### Kafka Data Producer (`send_turnstile_data.sh`)

This Bash script simulates real-time subway foot traffic by generating synthetic MTA turnstile events. It pushes these events to the Kafka topic `mta_turnstile_topic` using the Kafka console producer utility. The script is designed with the following features:

* **Realistic Rush Hour Simulation:** Increases event frequency during 7–10 AM and 5–8 PM on weekdays.
* **Weekend & Holiday Scaling:** Reduces traffic on weekends and major U.S. holidays by 50%–66%.
* **Weighted Station Sampling:** Simulates higher message volume for major hubs like `34 ST-HERALD SQ`, `WORLD TRADE CTR`, and `TIME SQ-42 ST`.
* **Custom Formatting:** Generates CSV-formatted lines with 11 fields (e.g., `STATION`, `DATE`, `ENTRIES`, `EXITS`).

Each message is sent every 3–5 seconds to mimic a continuous real-time stream, forming the foundation of our ingest pipeline.

---

### Kafka Consumer (`kafka_stream_consumer.ipynb`)

This notebook reads streaming data from Kafka, parses and structures it using PySpark SQL, and routes the output to two primary sinks:

* **MongoDB** — Stores processed events in `mta_db.raw_turnstile` for long-term access.
* **Console/Memory Sink** — Prints batch records to the terminal for debugging and optionally allows SQL querying via in-memory tables.

Key components include:

* **Custom Schema Definition:** Ensures each CSV field is parsed to the correct type.
* **Spark Structured Streaming:** Handles streaming batch logic.
* **foreachBatch + writeStream:** Facilitates concurrent multi-sink output.
* **Timestamp Ingestion Column:** Adds `ingest_time` to support real-time freshness metrics.

This module acts as the live ETL layer, bridging the gap between synthetic event generation and downstream analysis/storage.

---

## Model Training with SparkML (`models/`)

### Training Notebook (`training_model.ipynb`)

This module loads historical data from MongoDB and builds predictive models for subway entries and exits using PySpark MLlib. Key operations include:

* **Feature Engineering:** Extracts hour, day of week, and numeric station index.
* **Vector Assembly:** Combines features into a single input vector via `VectorAssembler`.
* **Model Training:** Uses `RandomForestRegressor` to train two independent models for `ENTRIES` and `EXITS`.
* **Evaluation:** Uses RMSE and feature importance to validate models.
* **Serialization:** Saves trained `PipelineModel` for future inference.

Insights:

* **Station ID and hour** were the most influential predictors.
* The pipeline is modular and scalable, allowing for model retraining as new data is streamed in.

---

## Real-Time Foot Traffic Forecasting (`streaming/`)

### Streaming Prediction (`stream_consume_predict.ipynb`)

This notebook applies the trained models to live data coming from the Kafka stream. It performs real-time predictions of foot traffic by station, hour, and day. The steps include:

* **Kafka Subscription:** Subscribes to the `mta_turnstile_topic` and reads value fields.
* **Feature Extraction:** Derives `hour` and `day_of_week` from timestamp.
* **Model Loading:** Loads saved `PipelineModel` objects for both entries and exits.
* **Batch Inference:** Applies models to each mini-batch via `foreachBatch()`.
* **Output Rendering:** Joins prediction results and displays them in the console.

Sample Prediction Output:

```
| STATION         | hour | day_of_week | predicted_ENTRIES | predicted_EXITS |
|-----------------|------|-------------|-------------------|------------------|
| 34 ST-HERALD SQ | 11   | 2           | 8.76              | 8.96             |
| WORLD TRADE CTR | 11   | 2           | 8.32              | 8.71             |
```

This notebook demonstrates how batch-trained models can drive live inference, providing city-scale transit insights with near real-time latency.

---

## (EXTRA) Aggregated Modeling Variant (`aggregate_model_v2/`)

* Extended pipeline to use time-based and station-based aggregates
* Supports grouped station-hour analysis
* Trains and infers using enriched temporal-spatial groupings

---

## Lessons Learned

* Schema alignment between training and streaming is crucial in SparkML
* Kafka simulation logic (rush hour, weekend, holiday) improves data realism
* MongoDB enables efficient handling of semi-structured streaming data

---

## Sample Prediction Output

```
| STATION         | hour | day_of_week | predicted_ENTRIES | predicted_EXITS |
|-----------------|------|-------------|-------------------|------------------|
| 34 ST-HERALD SQ | 11   | 2           | 8.76              | 8.96             |
| WORLD TRADE CTR | 11   | 2           | 8.32              | 8.71             |
```

---

## Data Source

* [MTA Turnstile Dataset](https://data.ny.gov/Transportation/MTA-Subway-Turnstile-Usage-Data-2020/py8k-a8wg/about_data) (\~13M records)

---

## Future Improvements

* Streamlit dashboard for real-time monitoring
* Cross-validation for hyperparameter tuning
* Integration with weather/event APIs
* Anomaly detection on foot traffic surges

---

Built by Gopala Krishna Abba and collaborators 
