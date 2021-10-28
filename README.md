# capstone
Overview: 

The Data Quality Solution provides an industry-developed best practices guide for the improvement of data quality and allows companies to better leverage their data quality programmes and to ensure a continuously improving cycle for the generation of master data. It details the crucial processes and capabilities that help organisations improve their data quality and maintain a sustainable good quality data output. 

Measuring data quality is critical to understand if you want to use enterprise data confidently in operational and analytical applications. Only good quality data can power accurate analysis, which in turn can drive trusted business decisions. 

Bad quality data impacts an organization’s business strategy of fuelling growth and driving innovation. The immediate concern is how an organization can measure data quality and find ways to improve it.  

Six data quality dimensions to assess: 

Dimension         |How it’s measured 
                  |
Accuracy          |How well does a piece of information reflect reality? 
                  |
Completeness      |Does it fulfill your expectations of what’s comprehensive? 
                  |
Consistency       |Does information stored in one place match relevant data stored elsewhere? 
                  |
Timeliness        |Is your information available when you need it? 
                  |
Validity          |Is information in a specific format, does it follow business rules, or is it in an unusable format? 
                  |
Uniqueness        |Is this the only instance in which this information appears in the database? 

 

The purpose of this solution is to help the end-user execute and manage all the test cases according to the business requirement with minimal effort. The results from the solution can be captured into Storage folder to view or download. This helps in getting an understanding of the source data. 

Problem Statement: 

1.Store source Data files in the file folder within storage account. 

2.Extract data from the storage account into the Data lake using the spark notebooks. 

3.Perform Data Quality checks on the extracted data and log the information into a storage folder. 

4.Identify the errors in the data and save the results in the specific folders based on the condition. 

Ex: 

>If the file doesn’t have any error rows the process the file and save the results into a folder location 

>If the file has less than 5% error records then log the errors, by capturing the error records into a specific folder location for this case. 

>If the file has greater than 5% error records, then Fix the error and re-process the data. 
