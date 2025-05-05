# NYC Subway Foot Traffic Prediction & Forecasting

## Executive Summary

This project implements a real-time big data pipeline to analyze and forecast passenger foot traffic across NYC subway stations using MTA turnstile data. By simulating and ingesting real-time data using Apache Kafka, transforming it with PySpark, and storing it in MongoDB, we built predictive models using SparkML to support smarter transit operations.

**Tech Stack:**
Apache Kafka, Apache Spark (Structured Streaming & MLlib), MongoDB, Python, Bash

---

## Project Structure

```
nyc-subway-foot-traffic-prediction-and-forecasting/
├── eda_sparkml_analysis/           # EDA + historical modeling using SparkML
├── kafka/                          # Kafka simulation and consumer pipeline
├── models/                         # Historical training notebooks
├── streaming/                      # Live inference on streamed data
├── aggregate_model_v2/            # Aggregated foot traffic pipeline (entries + exits)
├── send_turnstile_data.sh         # Kafka data simulator script
├── .gitignore
├── README.md
└── requirements.txt
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

```bash
1. Upload CSV data files to the notebook directory
2. Run the setup cells to install and configure Apache Spark
3. Execute EDA sections sequentially:
   - Data loading
   - Data cleaning and transformation
   - Visualization (Pandas/Seaborn, then PySpark)
4. Proceed with ML model training and evaluation cells
```

This historical modeling step was critical in establishing a baseline understanding of foot traffic behavior and ensuring the streaming predictions had a strong statistical foundation.

---

## Kafka Simulation & Streaming Ingestion (`kafka/`)

* **Kafka Producer (`send_turnstile_data.sh`)** simulates realistic turnstile events based on:

  * Rush hour patterns
  * Weekend and holiday scaling
  * Weighted station selection (e.g., 34 ST-HERALD SQ, WORLD TRADE CTR)
* **Kafka Consumer Notebook** reads streamed data, parses schema using Spark SQL, and:

  * Writes to MongoDB (`mta_db.raw_turnstile`)
  * Streams to Console + Memory sink for debugging

---

##  Model Training with SparkML (`models/`)

* Loaded cleaned data from MongoDB
* Engineered features (temporal + spatial)
* Used PySpark’s `RandomForestRegressor` to train separate models for `ENTRIES` and `EXITS`
* Indexed station names using `StringIndexer`
* Used `PipelineModel.save()` to serialize models

---

## Real-Time Foot Traffic Forecasting (`streaming/`)

* Subscribed to `mta_turnstile_topic` using Spark Structured Streaming
* Parsed streamed events and engineered live features (`hour`, `day_of_week`)
* Loaded saved Random Forest models
* Applied batch prediction to generate real-time `predicted_ENTRIES` and `predicted_EXITS`
* Printed live prediction outputs per station to the console

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

## Setup Instructions

### Prerequisites

```bash
# Prerequisites
- Apache Kafka 2.13 (with Zookeeper)
- Apache Spark (with spark-sql-kafka and MongoDB connector)
- MongoDB
- Python 3.x
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

Built by Gopala Krishna Abba and collaborators 
