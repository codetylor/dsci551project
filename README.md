# DSCI551 Project

This is a course project of DSCI 551 .

**Instructor:** [Wensheng Wu](mailto:wenshenw@usc.edu)

**Authors:** `Yuhao Ban`, `Yuqi Xiao`, `Hopong Ng` (names not listed in order)

**Contact Us:** [ngbangusc@gmail.com](mailto:ngbangusc@gmail.com)

## Project Description: 

As we know, public security in Los Angeles is always a big problem. So we want to create a web application which can provide some alarming information for citizens to avoid the high-risk area or time period. In this project, we will use two datasets (Los Angeles historical crime data and weather data) to analyze the relationship between crime patterns and weather. Then, develop a web app that provides users with specified crime data responding to time, location, weather, etc, based on the user's geolocation.

## Links
[YouTube Demo](https://youtu.be/V95tban4iug )    
[Google drive](https://drive.google.com/drive/folders/1zCxStanO6wAj5d2N4U1YiBhPM1xc-m0X?usp=sharing)

## Summary of What We Did

- Use MySQL as a relational database and Firebase as a NoSQL cloud database for storing both original data and results from the spark computation. 
- Use spark dataframe to manipulate real-time data transformation, feature extraction and analysis
- Web application for search page shows the statistic based on three different searching criterias by real-time computation using spark
- The Explore page allows users to search and explore all tables we used in MySQL and firebase. Exploration can be done by aggregation, groupby or keyword searching. 
- Crime table in MySQL database and weather.json in firebase are integrated with date attributes. The integration case is based on the necessity of our spark program, which is taking weather, area(geolocation) and time interval as inputs for both of our homepage and search page. 

## System architecture:
![](https://i.ibb.co/nrnR8jb/Capture.png)
1. User visits the homepage of web app. Webpage sends user’s data to backend and get final results back showing in tables and charts.
2. Flask server submit computation job to spark and get a flag back
3. Spark retrieves data from Mysql database.
4. Spark retrieves weather data from Firebase and store the computation results back to Firebase
5. Web app explores raw data in Mysql
6. Web app explores raw and derived data in Firebase

## Team members and Contributions:
Name  | Background  | Contribution
 ---- | ----- | ------  
 Hopong Ng  | CS | computer network, data structure, backend design
 Yuhao Ban  | Informatics | relational database design, back-end data processing and analysis  
 Yuqi Xiao  | Applied Mathematics and Statistics | Data preparation and firebase databas

## Reference
https://openweathermap.org/  
https://www.kaggle.com/cityofLA/crime-in-los-angeles






