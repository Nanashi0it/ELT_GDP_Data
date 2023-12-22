# Project Scenario: 
An international firm that is looking to expand its business in different countries across the world has recruited you. 
You have been hired as a junior Data Engineer and are tasked with creating an automated script that can extract the list of all countries in order of their GDPs in billion USDs (rounded to 2 decimal places), 
as logged by the International Monetary Fund (IMF). Since IMF releases this evaluation twice a year, this code will be used by the organization to extract the information as it is updated.

The required data seems to be available [here]('https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29')

# Objectives
- Write a data extraction function to retrieve the relevant information from the required URL.
- Transform the available GDP information into 'Billion USD' from 'Million USD'.
- Load the transformed information to the required CSV file and as a database file.
- Run the required query on the database: display only the entries with more than a 100 billion USD economy.
- Log the progress of the code with appropriate timestamps.
