# Health-Monitoring-System

*This is a bigdata project done by CSED students, Divided into 3 milestones.*

## Milestone 1

![image](https://user-images.githubusercontent.com/58369917/177416371-f6b882f1-3cd2-44a4-8912-bbc566a21dff.png)

• it is required to build a mock microservices system in which
services send health messages to a certain client server. The client server should
receive these messages, catalog, and persist them on HDFS.

• We are required to build and deploy 4 services that send health messages to the
health monitor. For this milestone, the services should be simple. The services will
just send health messages to the Health Monitor system and perform no actual
functionality.

• The services will send messages to the Health Monitor system using the UDP
protocol.

• We are required to implement a server that receives the health messages and
directs them to be stored on HDFS.

• HDFS should be in cluster mode of 4 (size of group) machines. That is the
data will be distributed to 4 machines.


## Milestone 2

![image](https://user-images.githubusercontent.com/58369917/177419136-b1302281-8515-47e8-aa44-bb47e5334cfc.png)


• For this milestone, it is required to analyze the data stored inside HDFS. The health
reports for multiple services contain valuable data if we are going to monitor the
services status and health.

• We are required to calculate the below analytics over the data persisted in HDFS using Map Reduce jobs.
 - The mean CPU utilization for each service
 - The mean Disk utilization for each service
 - The mean RAM utilization for each service
 - The peak time of utilization for each resource for each service
 - The count of health messages received for each service

• We are required to design and implement the map reduce jobs that will calculate
the required statistics. The map step of any map-reduce job processes each record
individually producing a record or more as a result. The reduce step collects all
records having a common attribute and produces one record summarizing,
reducing, these records.

###### Backend
The backend should be simple with only one API exposed: a GET request specifying the window over which the analytics will be computed. The backend will start the map-reduce jobs to compute the required statistics.
       
       
###### Frontend
The frontend is a simple, single page application that contains a button with two date pickers to define the window ends. On click of the button, the frontend sends a GET request with the proper parameters to the backend requesting the needed analytics. The frontend will wait for the results and show them in any proper format.




## Milestone 3 - Lambda Architecture


![image](https://user-images.githubusercontent.com/58369917/177420382-a5278f5a-d53f-4362-b6b0-5f1b4369121c.png)

• For this milestone, it is required to use the lambda architecture for our health
monitor system. See the figure Above.

1. The health reports arriving from the health monitor datasource will get persisted as raw data in the Batch Layer using HDFS using CSV files.
2. Periodically the scheduler triggers Map-Reduce jobs to digest the HDFS data and construct Batch Views that represent the aggregated results. The output of the Map-Reduce jobs is saved as Parquet files.
3. Periodically the scheduler triggers Spark jobs to create Realtime Views that capture the recent data. The output of the Spark jobs is saved as Parquet files.
4. Both the Serving Layers and the Speed Layers use NoSQL databases to allow querying the views using SQL. DuckDB was used.
5. Whenever the Frontend submits a request to the Backend, the Backend sends SQL queries to both Serving Layers & Speed Layers and stitches the results together and sends it back to the Frontend.
6. The user is going to access your frontend from any browser and click a button to request some analytics functions over a window of time. The analytics functions that you need to implement are :
   - The mean CPU utilization for each service
   - The mean Disk utilization for each service
   - The mean RAM utilization for each service
   - The peak time of utilization for each resource for each service
   - The count of health messages received for each service


###### Batch Layer
Use implementation of Milestone 1 to process health messages
       
###### Serving Layer
Use implementation of Milestone 2 to generate Batch Views using Map-Reduce. But instead of triggering map reduce by every backend request, it will be triggered periodically controlled by a scheduler.

###### Speed Layer
- It is required to design a speed layer that has an input stream of health messages and outputs and stores the current analytical results of the required analytics in the form of Parquet files. The scheduler will trigger running the Spark jobs to generate the Realtime Views.
- We needed to expire Realtime Views that are already consumed in the Serving Layer. That will require to maintain two sets of the Realtime Views and alternate between them
       

###### Backend
Remains the same but in addition, The backend will collect query results by contacting both speed layer and batch views to aggregate the results and stitch them together.

###### Frontend
Remains the same but in addition, time picker is added with the date for more precision.



