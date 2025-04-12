# AWS-Supermarket-Data-Project

## 1. Introduction & Purpose

This project simulates a real-time data engineering setup that could run in a cloud environment (like AWS) to manage transaction logs for multiple stores (for example, a chain of supermarkets). It shows how you can combine **historical (batch) data** with **real-time (streaming) data**, store and query it for cheap, and see it all in near real-time.

The main idea is to have a scalable, low-cost pipeline that can handle data from many stores each second, then make that data available for analysis and dashboards within a few seconds (2–3 seconds after a transaction arrives). Even if there’s a lot of data, the cost stays small compared to the total revenue of a typical retail business.

---

## 2. High-Level Architecture

1. **Historical Data**

   - Stored as four relational tables (explained later) in Amazon S3.
   - Queried directly in Amazon Athena, which reads these tables from S3.
   - Originally populated by Python scripts (to create data) and an AWS Glue job (to transform JSON logs into a relational format).

2. **Real-Time Data**

   - A Python script runs in AWS ECS (Elastic Container Service), acting like a payment processor by writing fresh transaction logs into an S3 bucket.
   - An Amazon S3 event trigger reacts whenever a new transaction file lands in that folder. This trigger calls an AWS Lambda function.
   - The Lambda function reads the raw JSON, changes it to the right format, and appends rows to the **transactions** and **transaction_items** tables (kept in separate folders in S3).
   - After that, these new records can be queried in Amazon Athena and seen in Amazon QuickSight right away. Just refresh QuickSight to view the latest data.

3. **Visualization & Analytics**

   - Data is viewed in Amazon QuickSight dashboards.
   - When a new transaction shows up, it usually takes just 2–3 seconds for it to be processed and appear on the dashboard.
   - You can also use Athena for deeper queries, joining transaction data with SKUs and store info.

---

## 3. Historical / Static Data (Four Relational Tables)

All past data is stored in an Amazon S3 bucket but organized as **four relational tables**. Athena can query them like standard database tables. Below is a rundown of each:

### 2.1 `supermarkets` Table

- **Columns**:
  1. `store_id` – A unique ID for each store (e.g., `STORE0003`).
  2. `store_name` – A randomly created store name (e.g., "Main Street Supermarket").
  3. `store_location` – A randomly generated location or city for the store.
- **Explanation**: This table holds basic details about each store in the chain. In this project, five stores are created by a Python script, but it can scale to any number of stores.

### 2.2 `skus` Table

- **Columns** (example set):
  1. `sku_id` – A unique ID for each product (e.g., `SKU07067`).
  2. `product_name` – A randomly created product name.
  3. `price` – A randomly chosen or assigned price.
- **Explanation**: Contains info for each Stock Keeping Unit (SKU). We generated about 12,000 SKUs with a Python script, though this can be scaled up or down.

### 2.3 `transactions` Table

- **Columns** (example set):
  1. `transaction_id` – A unique ID for each transaction (e.g., `TX0000001`).
  2. `store_id` – Links to the `supermarkets.store_id` where the sale happened.
