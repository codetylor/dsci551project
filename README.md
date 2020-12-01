# DSCI551 Project

This is a course project of DSCI 551 .

**Instructor:** [Wensheng Wu](mailto:wenshenw@usc.edu)

**Authors:** `Yuhao Ban`, `Yuqi Xiao`, `Hopong Ng` (names not listed in order)

**Contact Us:** [ngbangusc@gmail.com](mailto:ngbangusc@gmail.com)

**Reference:** listed in each section if needed.

## Project Description: 

As we know, public security in Los Angeles is always a big problem. So we want to create a web application which can provide some alarming information for citizens to avoid the high-risk area or time period. In this project, we will use two datasets (Los Angeles historical crime data and weather data) to analyze the relationship between crime patterns and weather. Then, develop a web app that provides users with specified crime data responding to time, location, weather, etc, based on the user's geolocation.

## Summary of What We Did

- Use MySQL as a relational database and Firebase as a NoSQL cloud database for storing both original data and results from the spark computation. 
- Use spark dataframe to manipulate real-time data transformation, feature extraction and analysis
- Web application for search page shows the statistic based on three different searching criterias by real-time computation using spark
- The Explore page allows users to search and explore all tables we used in MySQL and firebase. Exploration can be done by aggregation, groupby or keyword searching. 
- Crime table in MySQL database and weather.json in firebase are integrated with date attributes. The integration case is based on the necessity of our spark program, which is taking weather, area(geolocation) and time interval as inputs for both of our homepage and search page. 

## System architecture:
![](https://i.ibb.co/xCKkF9L/Capture.png)
