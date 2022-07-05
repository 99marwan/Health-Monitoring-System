# Health-Monitoring-System

*This is a bigdata project done by CSED students, Divided into 3 milestones.*

## Milestone 1

![image](https://user-images.githubusercontent.com/58369917/177416371-f6b882f1-3cd2-44a4-8912-bbc566a21dff.png)

•it is required to build a mock microservices system in which
services send health messages to a certain client server. The client server should
receive these messages, catalog, and persist them on HDFS.

•We are required to build and deploy 4 services that send health messages to the
health monitor. For this milestone, the services should be simple. The services will
just send health messages to the Health Monitor system and perform no actual
functionality.

•The services will send messages to the Health Monitor system using the UDP
protocol.

•We are required to implement a server that receives the health messages and
directs them to be stored on HDFS.

•HDFS should be in cluster mode of 4 (size of group) machines. That is the
data will be distributed to 4 machines.


## Milestone 2

![image](https://user-images.githubusercontent.com/58369917/177419136-b1302281-8515-47e8-aa44-bb47e5334cfc.png)


•For this milestone, it is required to analyze the data stored inside HDFS. The health
reports for multiple services contain valuable data if we are going to monitor the
services status and health.

•We are required to calculate the below analytics over the data persisted in HDFS using Map Reduce jobs.
 - The mean CPU utilization for each service
 - The mean Disk utilization for each service
 - The mean RAM utilization for each service
 - The peak time of utilization for each resource for each service
 - The count of health messages received for each service

•We are required to design and implement the map reduce jobs that will calculate
the required statistics. The map step of any map-reduce job processes each record
individually producing a record or more as a result. The reduce step collects all
records having a common attribute and produces one record summarizing,
reducing, these records.

###### Backend
       The backend should be simple with only one API exposed: a GET request specifying the window over which the analytics will be computed. The backend will start the map-reduce jobs to compute the required statistics.
       
       
###### Frontend
       The frontend is a simple, single page application that contains a button with two date pickers to define the window ends. On click of the button, the frontend sends a GET request with the proper parameters to the backend requesting the needed analytics. The frontend will wait for the results and show them in any proper format.









