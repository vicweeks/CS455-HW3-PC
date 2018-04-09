# CS455-HW3-PC
CS455 Homework 3: Programming Component

### Description
* Each folder in ```programs``` represents an individual program for this project. The ```HW3``` folder combines programs Q1-Q6 into a single map reduce program which writes the output for each problem into individual folders.

* Build the project using ant, then run with Hadoop. For example, to run HW3 use the command ```$HADOOP_HOME/bin/hadoop jar dist/HW3.jar cs455.hadoop.hw3.HW3Job```. This assumes access to original cluster for this project.

Note: Each file contains a description of usage.


### Q7 Description
* For Q7 I decided to pull datasets of extreme weather events in the US which occured bewtween 1987 and 2008. The datasets were from the National Centers for Environmental Information, National Oceanic and Atmospheric Administration (https://www1.ncdc.noaa.gov/pub/data/swdi/stormevents/csvfiles/). I preprocessed the data to extract info about the state, year, month, day, time(hour), and event_type for each event. I then compared the Ontime Performance datasets to these entries to identify major events that occured near the same time as a weather delay. I output the event types and number of occurances for each state, for all states together, and the top event for each individual state.