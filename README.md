
# Facets Overview Spark

Google Open Sourced the Facets Project in 2017 (https://github.com/PAIR-code/facets), which can help Data Scientists to better understand the data set 
and under the slogan that "Better data leads to better models".

The Google "Facets" includes two sub-projects: Facets-overview and Facets-dive.
 
"Facets Overview "takes input feature data from any number of datasets, analyzes them feature by feature and visualizes the analysis.

Based on Facets( github page: https://github.com/PAIR-code/facets)

```
Overview gives a high-level view of one or more data sets. It produces a visual feature-by-feature statistical analysis, and can also be used to compare statistics across two or more data sets. The tool can process both numeric and string features, including multiple instances of a number or string per feature.
Overview can help uncover issues with datasets, including the following:

* Unexpected feature values
* Missing feature values for a large number of examples
* Training/serving skew
* Training/test/validation set skew
Key aspects of the visualization are outlier detection and distribution comparison across multiple datasets. Interesting values (such as a high proportion of missing data, or very different distributions of a feature across multiple datasets) are highlighted in red. Features can be sorted by values of interest such as the number of missing values or the skew between the different datasets.

```

The Facets-overview is consists of 

* Feature Statistics Protocol Buffer
* Feature Statistics Generation
* Visualization

The implementation of Feature Statistics Generation have two flavors : Python (used by Jupyter notebook) and Javascripts (used by web )

![Overview visualization](src/main/images/facets_overview.png)


Current implemention is in python depends on Numpy or Javascripts. Which means the data statics generation is limited by 
one machine or one browser. 

This project provides an additional implementation for Statistics generation. 
We can use Spark to leverage the spark generate stats with distributed computinig capability

![Overview visualization with Spark](src/main/images/facets_overview_spark.png)


## Design Considerations
* We need to find a Scala Protobuf generator
* The python implementation is in loop-mutable and update fasion, we need re-arrange it to use immutable collection
* Numpy Implementation load all the data is in Array on the current computer, we need some way to transform data without load all data into memory.
* We also need to find equivalent Numpy functions such as avg, mean, stddev, histogram
* The "overview" implementation can be apply to Tensorflow Record (tf.sampels, tf.sequenceSamples), where the data value could be array or array of array of data. 
* In this implementation, we can leveraging tensorflow/ecosystem/spark-tensorflow-connector for tensorflow record support, where we can load the TFRecords into spark DataFrame. Once the DataFrame is created, rest of implementation is almost same.
* We use DataFrame to represent the Feature. this is equivalent the feature array used in the Numpy. 
* Efficiency is not the major concern in this first version of design, we may have to pass data in multiple passes.


## Data Structures

Based on Feature_statistics Protobuf definitions, 

* Each dataset has DatasetFeatureStatistics which contains lists metadata (name of dataset, number of sample, and Seq[FeatureNameStatistics])
* Each FeatureNameStatistics is feature stats for the given dataset, which includes feature name and data type and one of (NumericStatistics,StringStatistics or binary_stats)
* NumericStatistics consists CommonStatistics (std_dev,mean, median,min, max, histogram)
* StringStatistics consists CommonStatistics  as well as (unique values, {value, frequency}, top_values,avg_length, rank_histogram )

Google's Python implementation mainly use dictionary to hold data structures. In this implemenation, we define several additional data structures to help organize the data. 

* Each dataset can be presented by the NamedDataFrame
* Each dataset can be split with different features, so that it can also be converted to DataEntrySet
* Each DataEntry represents one feature with basic information as well the DataFrame for the feature.
* Each Feature associate with FeatureNameStatistics defined above, we use BasicNumStats and BasicStringStats to capture the basic statistics

```
case class NamedDataFrame(name:String, data: DataFrame)
case class DataEntrySet(name: String, size: Long, entries : Array[DataEntry])
case class DataEntry(featureName: String,
                     `type` : ProtoDataType,
                     values:DataFrame,
                     counts: DataFrame,
                     missing : Long,
                     feat_lens: Option[DataFrame] = None
                    )
case class BasicNumStats(name: String,
                         numCount: Long = 0L,
                         numNan :Long = 0L,
                         numZeros:Long = 0L,
                         numPosinf:Long = 0,
                         numNeginf: Long = 0,
                         stddev : Double = 0.0,
                         mean   : Double = 0.0,
                         min    : Double = 0.0,
                         median : Double = 0.0,
                         max    : Double = 0.0,
                         histogram: (Array[Double], Array[Long])
                        )

case class BasicStringStats(name: String,
                            numCount: Long = 0,
                            numNan :Long = 0L
 )
 
 
```

Main  class

```
class FeatureStatsGenerator(datasetProto: DatasetFeatureStatisticsList) {

 def protoFromDataFrames(dataFrames: List[NamedDataFrame],
                         features : Set[String] = Set.empty[String],
                         histgmCatLevelsCount:Option[Int]=None): DatasetFeatureStatisticsList = ???
  

}


 
``` 

## FeatureStatsGenerator

### Build
```
    mvn clean install
```

### Test
```
    mvn test
```

### License
```
    Apache License 2.0
```

//todo: add maven depedency usage
//todo : add code to package to update to maven repository
//todo: add code to show how to use jupyter notebook
//todo: add code to show how to use it in Javascripts


## Usage Smaples

   ### Generate Protobuf from DataFrame (using CSV)

   In this example, load the CSV file (adult.data.csv, adult.test.csv) into Spark DataFrame
   then pass the DataFrame to FeatureStatsGeneator to generate the protobuf class.

   The file can then be persisted into a binary protobuf file, or based64 encoded binary protobuf file.

   Few utility functions for loading from CSV and persist to file are provided

   As you can see, once the DataFrame is created, the rest of the code is the same.

   The examples can easily used for DataFrames from JSON, SQL, Tensorflow Records etc.

   For instances, I can simply change the functions to

   ```
    val trainData: DataFrame = loadJSONFile("path/to/data/jsonfile")
    val testData : DataFrame  = loadJSONFile("path/to/test/jsonfile")

    val trainData: DataFrame = sqlContext.sql("select * from DataTable")
    val testData : DataFrame = sqlContext.sql("select * from TestTable")

   ```

   Here is the example:

   First load CSV files (data and test) into DataFrames

```
    val features = Array("Age", "Workclass", "fnlwgt", "Education", "Education-Num", "Marital Status",
                         "Occupation", "Relationship", "Race", "Sex", "Capital Gain", "Capital Loss",
                         "Hours per week", "Country", "Target")

    val trainData: DataFrame = loadCSVFile("src/test/resources/data/adult.data.csv")
    val testData : DataFrame = loadCSVFile("src/test/resources/data/adult.test.txt")
```
    Next Associate the column names ("schema") to the loaded DataFrame, then created a
    list of "Named DataFrame"

```
    val spark = sqlContext.sparkSession
    import spark.implicits._

    val train = trainData.toDF(features: _*)
    val test = testData.toDF(features: _*)

    val dataframes = List(NamedDataFrame(name = "train", train), 
                          NamedDataFrame(name = "test", test))

```
   Next, we create FeatureStatsGenerator, and passed the namedDataFrame list to the generator,
   then call ```protoFromDataFrames(dataframes)``` to generate the stats.

```
    val generator = new FeatureStatsGenerator(DatasetFeatureStatisticsList())
    val proto = generator.protoFromDataFrames(dataframes)
```
    Once we have the feature stats probuf, we can save it to file, which can be loaded into web
    or jupyter notebook.


```
    persistProto(proto,base64Encode = false, new File("src/test/resources/data/stats.pb"))
    persistProto(proto,base64Encode = true, new File("src/test/resources/data/stats.txt"))
```

   Here are some utility functions

```
  
  private def loadCSVFile(filePath: String) : DataFrame = {
    val spark = sqlContext.sparkSession
    spark.read
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .option("header", "false") //reading the headers
      .option("mode", "DROPMALFORMED")
      .option("inferSchema", "true")
      .load(filePath)
  }

  
  private def persistProto(proto: DatasetFeatureStatisticsList, base64Encode: Boolean = false, file: File ) = {
    if (base64Encode) {
      import java.util.Base64
      val b = Base64.getEncoder.encode(proto.toByteArray)
      import java.nio.charset.Charset
      import java.nio.file.{Files, Paths}
      val  UTF8_CHARSET = Charset.forName("UTF-8")

      Files.write(Paths.get(file.getPath), new String(b, UTF8_CHARSET).getBytes())
    }
    else {
      Files.write(Paths.get(file.getPath), proto.toByteArray)
    }
  }

```

### Generating stats for SequenceExample data

    In this example, we generate feature stats for the DataFrame
    that is the same as TFRecord SequenceExample

    The protobuf is then print to Json for easy to read.
```
     val data = Seq((Seq(Seq("Tim Robbins","Morgan Freeman"), Seq("Brad Pitt","Edward Norton","Helena Bonham Carter")),
                        Seq(Seq("The Shawshank Redemption"),Seq("Fight Club")),
                        Seq(Seq(9.0), Seq(9.7)),
                        19L,
                        List("Majesty Rose", "Savannah Outen", "One Direction"),
                        "pt_BR"
                      ))
        val featureNames = Seq("Movie Actors" ,"Movie Names",  "Movie Ratings","Age" ,"Favorites",  "Locale")
        val dataDF = spark.createDataFrame(data).toDF(featureNames:_*)


        dataDF.printSchema()
        dataDF.show()
        //generate datastats
        val dataframes = List(NamedDataFrame(name = "data", dataDF))
        val p = generator.protoFromDataFrames(dataframes)

        println("json=" + toJson(p))


  private def toJson(proto: DatasetFeatureStatisticsList) : String = {
    import scalapb.json4s.JsonFormat
    JsonFormat.toJsonString(proto)
  }

```

### Generate Stats for TFRecords

```
 val spark = sqlContext.sparkSession

 val schema = StructType(List(StructField("id", IntegerType),
              StructField("IntegerTypeLabel", IntegerType),
              StructField("LongTypeLabel", LongType),
              StructField("FloatTypeLabel", FloatType),
              StructField("DoubleTypeLabel", DoubleType),
              StructField("VectorLabel", ArrayType(DoubleType, containsNull =  true)),
              StructField("name", StringType)))


 val df =  TFRecordHelper.loadTFRecords(spark, path,  TFRecordType.Example, Some(schema))

 val p  = generator.protoFromDataFrames(dataframes)

 println("json=" + toJson(p))

```

 The TFRecordHelper is small utility class which allow your to use leverage enumerated TFRecordType
 and pass-in optional schema




