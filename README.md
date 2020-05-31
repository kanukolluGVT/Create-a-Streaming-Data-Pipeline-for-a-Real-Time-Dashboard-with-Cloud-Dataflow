# Create-a-Streaming-Data-Pipeline-for-a-Real-Time-Dashboard-with-Cloud-Dataflow
 coursera data enginering


Overview
In this lab, you own a fleet of New York City taxi cabs and are looking to monitor how well your business is doing in real-time. You will build a streaming data pipeline to capture taxi revenue, passenger count, ride status, and much more and visualize the results in a management dashboard.

Open BigQuery Console
In the Google Cloud Console, select Navigation menu > BigQuery:

BigQuery_menu.png

The Welcome to BigQuery in the Cloud Console message box opens. This message box provides a link to the quickstart guide and lists UI updates.

Click Done.

Note your Project Name; Confirm that Needed APIs Are Enabled
Make a note of the name of your GCP project. This value is shown in the top bar of the GCP Console.

In the GCP Console, in the Navigation menu, click Home.

In the Project Info section, copy and save your Project ID value for later use. Your project ID will resemble qwiklabs-gcp-d2e509fed105b3ed.

In the GCP Console, in the Navigation menu, click APIs & services.

Console_APIs.png

Scroll down in the list of enabled APIs, and confirm that these APIs are enabled:

Google Cloud Pub/Sub API

Dataflow API

If one or more API is not enabled, click the ENABLE APIS AND SERVICES button at the top. Search for the APIs by name and enable each API for your current project.

Create a Cloud Pub/Sub Topic
Cloud Pub/Sub is an asynchronous global messaging service. By decoupling senders and receivers, it allows for secure and highly available communication between independently written applications. Cloud Pub/Sub delivers low-latency, durable messaging.

In Cloud Pub/Sub, publisher applications and subscriber applications connect with one another through the use of a shared string called a topic. A publisher application creates and sends messages to a topic. Subscriber applications create a subscription to a topic to receive messages from it.

Google maintains a few public Pub/Sub streaming data topics for labs like this one. We'll be using the NYC Taxi & Limousine Commission’s open dataset

Create a BigQuery dataset
BigQuery is a serverless data warehouse. Tables in BigQuery are organized into datasets. In this lab, messages published into Pub/Sub will be aggregated and stored in BigQuery.

To create a new BigQuery dataset:

Option 1: Command Line
Open Cloud Shell and run the below command to create the taxirides dataset

bq mk taxirides
Run this command to create the taxirides.realtime table (empty schema we will stream into later)
bq mk \
--time_partitioning_field timestamp \
--schema ride_id:string,point_idx:integer,latitude:float,longitude:float,\
timestamp:timestamp,meter_reading:float,meter_increment:float,ride_status:string,\
passenger_count:integer -t taxirides.realtime
Option 2: BigQuery Console UI
Skip these steps if you created the tables using the command line

In the GCP Console, go to Navigation menu> BigQuery.

Once there, click on your GCP Project ID from the left-hand menu

Now on the right-hand side of the console underneath the query editor click CREATE DATASET.

Give the new dataset the name taxirides, leave all the other fields the way they are, and click Create dataset.

If you look at the left-hand resources menu, you should see your newly created dataset

Click on the taxirides dataset

Click create table

Name the table realtime

For schema, click edit as text and paste in the below

ride_id:string,
point_idx:integer,
latitude:float,
longitude:float,
timestamp:timestamp,
meter_reading:float,
meter_increment:float,
ride_status:string,
passenger_count:integer
Under Partition and cluster settings, select the timestamp option for the Partitioning field.

Confirm against the below screenshot:

bq-taxi-table.png

Click the Create Table button.
Create a Cloud Storage Bucket
Skip this step if you already have a bucket created

Cloud Storage allows world-wide storage and retrieval of any amount of data at any time. You can use Cloud Storage for a range of scenarios including serving website content, storing data for archival and disaster recovery, or distributing large data objects to users via direct download. In this lab we will use Cloud Storage to provide working space for our Cloud Dataflow pipeline.

In the GCP Console, go to Navigation menu > Storage.
Click CREATE BUCKET.
For Name, paste in your GCP project ID.
For Default storage class, click Multi-regional if it is not already selected.
For Location, choose the selection closest to you.
Click Create.
Set up a Cloud Dataflow Pipeline
Cloud Dataflow is a serverless way to carry out data analysis. In this lab, you will set up a streaming data pipeline to read sensor data from Pub/Sub, compute the maximum temperature within a time window, and write this out to BigQuery.

In the GCP Console, go to Navigation menu > Dataflow.

In the top menu bar, click CREATE JOB FROM TEMPLATE.

Enter streaming-taxi-pipeline as the Job name for your Cloud Dataflow job.

Under Cloud Dataflow template, select the Cloud Pub/Sub Topic to BigQuery template.

Under Cloud Pub/Sub input topic, enter projects/pubsub-public-data/topics/taxirides-realtime

Under BigQuery output table, enter <myprojectid>:taxirides.realtime

Note: there is a colon : between the project and dataset name and a dot . between the dataset and table name

Under Temporary Location, enter gs://<mybucket>/tmp/

Click the Run job button.

A new streaming job has started! You can now see a visual representation of the data pipeline.

dataflow-taxi-setup.png

Analyze the Taxi Data Using BigQuery
To analyze the data as it is streaming:

In the GCP Console, open the Navigation menu and select BigQuery.

Enter the following query in the Query editor and click RUN:

SELECT * FROM taxirides.realtime LIMIT 10
If no records are returned, wait another minute and re-run the above query (Dataflow takes 3-5 minutes to setup the stream). You will receive a similar output:
taxi-bq-output.png

Perform aggregations on the stream for reporting
Copy and paste the below query and run


WITH streaming_data AS (

SELECT
  timestamp,
  TIMESTAMP_TRUNC(timestamp, HOUR, 'UTC') AS hour,
  TIMESTAMP_TRUNC(timestamp, MINUTE, 'UTC') AS minute,
  TIMESTAMP_TRUNC(timestamp, SECOND, 'UTC') AS second,
  ride_id,
  latitude,
  longitude,
  meter_reading,
  ride_status,
  passenger_count
FROM
  taxirides.realtime
WHERE ride_status = 'dropoff'
ORDER BY timestamp DESC
LIMIT 100000

)

# calculate aggregations on stream for reporting:
SELECT
 ROW_NUMBER() OVER() AS dashboard_sort,
 minute,
 COUNT(DISTINCT ride_id) AS total_rides,
 SUM(meter_reading) AS total_revenue,
 SUM(passenger_count) AS total_passengers
FROM streaming_data
GROUP BY minute, timestamp

The result shows key metrics by the minute for every taxi drop-off

Create a Real-Time Dashboard
Click Explore with Data Studio

Specify the below settings:

Chart type: column chart
Date range dimension: dashboard_sort
Dimension: dashboard_sort, minute
Drill Down: dashboard_sort
Metric: SUM() total_rides, SUM() total_passengers, SUM() total_revenue
Sort: dashboard_sort Ascending (latest rides first)
example-taxi-dashboard.png

Note: Visualizing data at a minute-level granularity is currently not supported in Data Studio as a timestamp. This is why we created our own dashboard_sort dimension.

When you're happy with your dashboard, click Save to save this data source

Whenever anyone visits your dashboard, it will be up-to-date with the latest transactions. You can try it yourself by clicking on the Refresh button near the Save button

Stop the Cloud Dataflow job
Navigate back to Cloud Dataflow

Click the streaming-taxi-pipeline

Click Stop Job and Cancel pipeline

This will free up resources for your project

Congratulations!
In this lab you Pub/Sub to collect streaming data messages from Taxis and feed it through your Dataflow pipeline into BigQuery.
