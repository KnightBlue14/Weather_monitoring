# Weather_monitoring
A project in which I used the OpenWeatherMap API to log current weather data into a MySQL database for storage, then ran operations using Spark and Tableau to analyse and create visualisations for the data

## Description
OpenWeatherMap is a UK based team of data scientists that provide an open-source medium for historical, current and forecast weather data via their API.

The goal of this project was to use their API to monitor 6 locations in the UK, extract and transform the data in python, then store it in a MySQL instance, to test the implementation of a program over several days, then pull the data in bulk for analysis. 

OpenWeatherMap do have several paid options for gathering data, but this project makes use of their free tier, which includes the API for current and forecast data, a geolocation API for additional information tied to the location, and an air pollution API to monitor the concentration of pollutants, such as carbon monoxide or ammonium. A more in-depth explanation will be included in the next section.

## The APIs

Each API fills a particular role, which may not be readily apparent from the files presented in this repository, so linked below are the pages for each one. For a breakdown of the variables tracked, as used by this project, scroll down to the 'JSON format API response fields' section for each.

https://openweathermap.org/current - the API for current wweather data at a targeted location, such as rain, cloud coverage, temperature, etc.

https://openweathermap.org/api/air-pollution - the API for air pollution data, such as the concentration of carbon monoxide. There is also reference to an 'Air Quality Index', which is explained here - https://openweathermap.org/air-pollution-index-levels

https://openweathermap.org/api/geocoding-api - the API for additional details about the location itself, including the state, country, and names of the city in multiple languages. This saw little use, as the cities monitored are all in England, though it is more useful if you monitor multiple countries and/or states.
## Technologies used
* Python - Python was used to pull data from the API, using a key provided to a free account.
* MySQL - A MySQL database was created to store the data from the API, with regular updates every 30 minutes.
## How to use

### Weather.py

The bulk of operations are done in this file - data is extracted, transformed, and then loaded into MySQL using mysql.connector. For your purposes, a few things will need to be adjusted. Rather than variable by variable, much of this will be by sections of code, as it can be quite in depth -
* API_key - here is where you will insert your API key, from your account. There are limits to be aware of, which will be explained below
* Lines 15-20 - These lines are where you will enter the details of the cities you will be monitoring. The 2 dictionaries, 'dic' and 'pops' store the latitude and longitude for each city, and the population of the city respectively. The former is used because that is what the api uses to determine the location, so this is used in conjunction with a list for use in a loop. The latter was used for the sake of analysis, but you could also collect it seperately, or not at all, depending on your use case. Population figures and co-ordinates were both found here - https://latitude.to/. 
* Lines 22-26 - For testing purposes, the script is set to call each API in turn, saving the results for each into its own dataframe, joining them together at the end after all transformations have been applied. For this, three lists of column names are stored in these lines, seperately from the dataframes, as those are reset with each iteration of the loop. You will need to add or delete names per your needs.
* Lines 32-35 - Details for connecting to your database. The username can be left to 'root', though you are also free to change the username. Just be sure that it is a registered user in your database.
* Lines 90-126 - Each line calls part of the saved API request, then assigns it to a column in the dataframes, which are reset with each iteration, resulting in multiple tables with the same layout for each city. If you wish to add or remove variables, and have edited the column lists from earlier, those changes will need to be reflected here, or data will be assigned to the wrong column. They all appear in the same order, which will help with keeping track of what goes where.
* Lines 131-161 - This section covers the creation of the table for each city, if it doesn't already exist (132-139), and the addition of new data (146-161). Any changes to what is added will need to be included here. For instance, if you do not wish to include the Wind_Speed, you will need to remove it from - 
* columns2 (Line 23), the 2nd df.loc function (Line 105), the create_table function (Line 135) and the cur.execute function (Line 154).
* Likewise, if you want to add anything else, you will need to add it to all of these locations, to gather the data, assign it, and create a table that is ready to include it.

### Weather_pull.py 

This file is used to pull data from the MySQL database. This function could be included in the original file, but it would result in a csv file being written each time, so it was separated for efficiency. As before, you will need to update -
* Database details - as above
* columns1, columns2 and columns3 - any changes to what data is stored need to be changed here as well, though this is less in depth. You only need to edit the column name list relevant to you.

## Additional Notes
This will vary with the tier of account you use, but in this case, I used the free API, which entitled me to an upper limit of 1000 calls per day, at a rate of 60 calls per minute, as well as limiting what options I could access. As a result, I limited the rate of calls to every 30 minutes, for greater depth of data, but only used 6 cities. Depending on your case, you will need to assign your workflow accordingly. Pricing information can be found here - https://openweathermap.org/price
