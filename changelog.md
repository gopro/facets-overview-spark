0.3.8  bug fix. Handle null value

0.3.7  2021-04-10 cleanup
  * leverage HTML <link rel="import">  we imported facets-jupyter.html, as result, we don't need to keep 
    all the styles and CSS for javascripts code. We can need index.html 
  * make copy of data for easy demo presentation ( although the same file has multiple copies)
  * removed SQLContextFactory.scala as its no longer needed
  * Jupyter notebook also updated to reference <facets-jupyter.html> from google facets github instead of local copy

0.3.6  2021-04-06 change the date handling logic: instead of converting date to INT ( which use as timestamp), we convert 
      it to String instead. 

0.3.5 bug fix
  * fix field did not correctly specify data type after converted to numeric data frame
  * fix field values are all nulls, which code can't determine the category of the data type 
  * Note the date field converted to numerical field is based on the facets_overviews' covert np.datetime64 to INT 
    but now, I am not sure this is right approach to treat a date field as Integer field.
    as if the data is originally from a CSV file with Date String, it will be treated the as STRING, 
    but if the date is selected from Database where the Date String is stored in Date/Timestamp field, then current code 
    will convert it to numeric field and calculate of stats of the long value. Its not very helpful.  But I might fix it in next PR. 
  * add more tests
  
0.3.4 change Apache Spark dependency to 2.4.6 to 2.4.7 

0.3.3 move common used protobuf functions from test to ProtoUtils.scala

0.3.2 correct group Id misspelling

0.3.0 change the pom.xml to make Spark and Tensorflow as provided to reduce the size 
   of the jar file 

0.2.0 Upgrade dependencies
   * Apache Spark 1.x =>  2.4.6
   * Scalapb 0.7.0-rc4 => 0.9.8 
   * scalapb-json4s => 0.10.0 
   * spark-tensorflow-connector 1.6.0 => 1.15.0
   * tensorflow 1.8.0 => 1.15.0 
   * protobuf version 3.0.0 => 3.8.0
0.1.0 Init build 2018
     
      
     
  