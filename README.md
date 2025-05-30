# Lambda Architecture Project on Supermarket Sales Dataset

## 📄 Description
This project implements a Lambda Architecture to analyze and process historical sales data from a supermarket company. The dataset captures sales records from three branches over a period of three months, providing a robust basis for predictive analytics and market trend analysis. The architecture combines batch, speed, and serving layers to enable real-time and historical data processing for insightful analytics.

## Dataset Description
Source: Historical sales data of a supermarket company.
Data Period: 3 months.
Branches: Data is recorded across three distinct branches in highly populated cities.
Significance:
Facilitates the study of market competition.
Useful for predictive analytics to optimize sales strategies.
Provides a foundation for exploring customer behavior trends.

## Project Objectives
Batch Layer: Implement robust data processing pipelines for historical data analysis and storing precomputed views.
Speed Layer: Handle real-time data streams for low-latency, dynamic insights.
Serving Layer: Deliver combined results from batch and speed layers through an efficient query layer for end-user applications.

## Key Features
Supports real-time and batch data processing.
Provides actionable insights into supermarket sales trends.
Enables predictive analytics for optimizing business strategies.

## 🚀 Quick Start Guide

### Prerequisites

- Docker
- Docker Compose
- Minimum 32GB RAM recommended
- Git


### Installation Steps

1. **Clone the Repository**
   ```bash
   git clone https://github.com/Hajarita12/Lambda_Architechture
   
   ```

2. **Launch Infrastructure**
   ```bash
   docker-compose up -d
   ```
### Project Architecture
![image](https://github.com/user-attachments/assets/f78d6057-ada1-4cbb-a19c-2b46a1fe64c8)

### Tools and Technologies

- **Programming Language**: Python  
- **Batch Layer**: Apache Spark, Hive, Pyspark  
- **Speed Layer**: Apache Kafka, Spark Streaming  
- **Serving Layer**: Hive, Cassandra  
- **Machine Learning**: Spark ML, Pyspark  
- **Orchestration**: Apache Airflow  
- **Visualization**: Streamlit  
- **Containerization**: Docker for deploying the application  
