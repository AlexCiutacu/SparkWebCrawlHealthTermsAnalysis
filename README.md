# Project Description

This project leverages the power of Apache Spark to process and analyze a broad dataset drawn from a web crawl. The task at hand is to identify the frequency of certain health and fitness-related terms within the data.

In this project, I utilized two main stages to get the job done - data processing and analysis. The data processing began with reading data from a collection of WARC files using the WarcGzInputFormat and WarcWritable classes. I then cleaned this data and filtered out unnecessary noise, using regular expressions to eliminate any special characters or irrelevant terms. With the help of Spark's map-reduce functionality, I was able to calculate the occurrences of each target word within the dataset.

Following data processing, the analysis phase took place. I worked with the data from each monthly crawl segment, and while challenges arose, such as issues with input path and memory errors, they were addressed. By fine-tuning the Spark configuration and modifying resource allocation, I managed to optimize the program to overcome these obstacles.

As a result, the project successfully demonstrates the capabilities of Apache Spark in handling large datasets, offering a practical solution for real-world data analysis problems. The efficiency of Spark in processing and analyzing this data in a distributed manner also highlights its powerful computing potential.
