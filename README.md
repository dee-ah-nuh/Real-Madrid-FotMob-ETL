## Real Madrid Match Prediction Pipeline

🎯 Problem Statement
This project builds a production-grade machine learning pipeline to predict Real Madrid match outcomes (Win/Draw/Loss) in La Liga by leveraging historical match data and advanced performance metrics. The challenge lies in accurately forecasting soccer results—a notoriously unpredictable domain—by combining traditional statistical features (goals scored/conceded, home/away performance) with expected goals (xG) data that captures underlying team quality beyond final scores.

We tackle this by implementing an end-to-end data engineering solution: Python scrapers extract match-level data from FotMob and land it as JSON in S3

DuckDB serves as the data warehouse where dbt models engineer features like rolling averages, rest days, and opponent strength indicators; 

<!-- <idk yet>
PySpark jobs on AWS EMR transform raw data into partitioned Parquet files optimized for analytics;  -->

<!-- The entire pipeline is orchestrated via Airflow with idempotent design, automated validation comparing predictions against actual results, and infrastructure-as-code via Terraform—demonstrating modern data lake architecture, distributed processing with Spark, and scalable ML operationalization patterns. -->


#### Problem Considerations
---

- **Late Data StrategyProblem:** FotMob may not publish match results immediately after games end. Match times can be delayed. Opponent data may be missing.Our Approach:Watermarking vs Reprocessing Windows

- **Problem:** FotMob might add new stats (e.g., "progressive passes"), change field names, or restructure HTML.

- **Problem:** Bad data corrupts predictions. How do we catch issues before they break downstream consumers?


- **Problem** API Retries? How do we handle them? 