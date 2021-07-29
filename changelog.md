## change Log, most recent version on top

0.5.2  skip empty DataFrame when generating statistics

0.5.1  better handling data type exception & dependency version conflicts
   *   shade com.google.protobuf.* 
       according https://scalapb.github.io/docs/sparksql/
       "Spark ships with an old version of Google's Protocol Buffers runtime that is not 
        compatible with the current version. Therefore, we need to shade our copy of the 
        Protocol Buffer runtime."
   *   "Spark 3 also ships with an incompatible version of scala-collection-compat.", 
       although our unit test show passes, without any changes, but we experienced exceptions 
       when trying to convert protobuf to json using json4s. Even trying to shade scala-collection-compat
       doesn't help. here we did not shade the scala-collection-compact 
   *   instead of directly cast categorical data type to string, 
       the logic is changed to check the data type first before extract the value. 
       Also catch the unexpected exception to associate the feature with exception. 
       Many cases, where spark considered the data is string, if it is no explicitly cast to decimals. 
       this will help debugging which feature is the issue. 
   *   update readme     
       
   

0.5.0  update the dependency to support Spark 3.0.1 
   *   Spark 3 requires Scala 2.12, we have to few dependency upgrade as well, in particular,
       scala-maven-plugin, scalatest, spark-tensorflow-connector and scalapb
   *   scala-maven-plugin version 4.3.1 works, but higher version such as 4.4.x, 4.5.x do not work.
       If you use version 4.4.x, 4.5.x, you will get error identical to the one reported in : 
       http://5.9.10.113/66489291/project-build-with-circle-ci-is-failing
   *   with change to scalapb to the current version, the protobuf version argument is changed. 
       in scalapb 0.9.8 if the protobuf version is 3.8.0 then the argument is v380 and v261 for version 2.6.1
       in scalapb 0.11.0 we need to specify v3.8.0 for version 3.8.0
   *   spark-tensorflow-connector has no scala 2.12 releases at the moment, although the master branch does 
       has code for scala 2.12. Instead of waiting for the official release, we temporarily build the dependency ourselves. 
       We add git submodule of tensorflow/ecosystem, then build the dependency locally
       ```
           cd ecosystem/spark/spark-tensorflow-connector;
           mvn clean install 
       ```
   *   change scalatest dependency to version 3.0.5. 
       when change the scalatest to the later versions > "3.0.5", the tests will show errors like ```"object FunSuite is not a member of package org.scalatest"```  



0.4.1  README and change log changes, created the branch for v.0.4.1 Apache Spark 2.4.x development
       The main branch will be upgrade to Apache Spark 3.0 

0.4.0  set categorical feature histogram level to 20, instead of infinite. 
       This avoid user create a big protobuf size in a big dataset with a lot of categorical feature rows.   

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
     
      
     
  