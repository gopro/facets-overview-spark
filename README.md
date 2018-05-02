# Facets Overview Spark
 
* this code is intended to be give back to open-source community, therefore this is only temporary put here for testing
 
 
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
* Need to find a Scala Protobuf generator
* The python implementation is in loop-mutable and update fasion, we need re-arrange it to use  immutable container and collection fasion 
Numpy Implementation consider all the data is in Array on the current computer, we need some way to transform data without collect the data into an array
Need to find equivalent Numpy functions such as avg, mean, stddev, histogram
* The "overview" implementation can be apply to Tensorflow Record (tf.sampels, tf.sequenceSamples), where the data value clould be array or array of array of nested structure. 
In this Scala + Spark Implementation, we will not support tensorflow record for this implemenation
But we do allow the data value to be nested array. 
* We use DataFrame to represent the Feature. this is equivalent the feature array used in the Numpy. 
Efficiency is not the major concern in this first version of design, we may have to pass data in multiple passes. 


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
Special note: feat_lens is used for tensorflow records
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

## Usage Smaple

```

 

  test("integration") {
  
    val features = Array("Age", "Workclass", "fnlwgt", "Education", "Education-Num", "Marital Status",
                         "Occupation", "Relationship", "Race", "Sex", "Capital Gain", "Capital Loss",
                         "Hours per week", "Country", "Target")

    val trainData: DataFrame = loadCSVFile("src/test/resources/data/adult.data.csv")
    val testData = loadCSVFile("src/test/resources/data/adult.test.txt")

    val train = trainData.toDF(features: _*)
    val test = testData.toDF(features: _*)

    val dataframes = List(NamedDataFrame(name = "train", train), 
                          NamedDataFrame(name = "test", test))


    val generator = new FeatureStatsGenerator(DatasetFeatureStatisticsList())
    
    val proto = generator.protoFromDataFrames(dataframes)
    persistProto(proto,base64Encode = false, new File("src/test/resources/data/stats.pb"))
    persistProto(proto,base64Encode = true, new File("src/test/resources/data/stats.txt"))
  }
  
  private def loadCSVFile(filePath: String) : DataFrame = {
    val spark = sqlContext.sparkSession
    spark.read
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .option("header", "false") //reading the headers
      .option("mode", "DROPMALFORMED")
      .option("inferSchema", "true")
      .load(filePath)
  }

  private def toJson(proto: DatasetFeatureStatisticsList) : String = {
    import scalapb.json4s.JsonFormat
    JsonFormat.toJsonString(proto)
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

