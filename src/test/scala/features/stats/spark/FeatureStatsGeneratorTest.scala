package features.stats.spark

import java.io.File
import java.nio.file.{Files, Paths}
import java.text.SimpleDateFormat
import java.time.temporal.ChronoUnit

import featureStatistics.feature_statistics.Histogram.HistogramType.QUANTILES
import featureStatistics.feature_statistics.{DatasetFeatureStatisticsList, FeatureNameStatistics}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

/**
# ==============================================================================
  *# Licensed under the Apache License, Version 2.0 (the "License");
  *# you may not use this file except in compliance with the License.
  *# You may obtain a copy of the License at
  *#
  *#     http://www.apache.org/licenses/LICENSE-2.0
  *#
  *# Unless required by applicable law or agreed to in writing, software
  *# distributed under the License is distributed on an "AS IS" BASIS,
  *# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  *# See the License for the specific language governing permissions and
  *# limitations under the License.
  *# ==============================================================================
  */

class FeatureStatsGeneratorTest extends FunSuite with BeforeAndAfterAll{

  val appName = "protoGenerator"
  val SPARK_MASTER_URL = "local[2]"
  var sc: SparkContext = _
  var sqlContext: SQLContext = _
  val generator = new FeatureStatsGenerator(DatasetFeatureStatisticsList())
  var spark: SparkSession = _

  override protected def beforeAll(): Unit = {
    val sparkConf = new SparkConf().setMaster(SPARK_MASTER_URL).setAppName(appName)
    sc = SparkContext.getOrCreate(sparkConf)
    sqlContext = SqlContextFactory.getOrCreate(sc)
    spark = sqlContext.sparkSession


  }


  private def persistProto(proto: DatasetFeatureStatisticsList, base64Encode: Boolean, file: File ):Unit = {
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

  test("generateProtoFromDataFrame") {

    val data = Seq((1, "hi"), (2, "hello"), (3, "hi"))
    val df = spark.createDataFrame(data).toDF("TestFeatureInt", "TestFeatureString")
    val dataframes = List(NamedDataFrame(name = "testDataSet", df))

    val convertedType1 = generator.convertDataType(df.schema.fields.head.dataType.typeName)
    assert(convertedType1.isInt)

    val convertedType2 = generator.convertDataType(df.schema.fields.tail.head.dataType.typeName)
    assert(convertedType2.isString)

    val proto = generator.protoFromDataFrames(dataframes)

    assert(proto.datasets.length == 1)
    val testData = proto.datasets(0)
    assert("testDataSet" === testData.name)
    assert(3 === testData.numExamples)
    assert(2 === testData.features.length)


    val (numfeat, stringfeat) =
      if (testData.features.head.name == "TestFeatureInt") {
        (testData.features.head, testData.features(1))
      }
      else {
        (testData.features(1), testData.features.head)
      }

    println(s"numfeat = ${numfeat.name}")
    println(s"numtype = ${numfeat.`type`}")
    println(s"stringfeat = ${stringfeat.name}")
    println(s"strtype = ${stringfeat.`type`}")

    assert("TestFeatureInt" === numfeat.name)
    assert(FeatureNameStatistics.Type.INT === numfeat.`type`)
    assert(1 === numfeat.getNumStats.min)
    assert(3 === numfeat.getNumStats.max)
    assert("TestFeatureString" === stringfeat.name)
    assert(FeatureNameStatistics.Type.STRING === stringfeat.`type`)
    assert(2 === stringfeat.getStringStats.unique)


  //  println(proto.toString)
  //  persistProto(proto)
  //  val r = toJson(proto)
  //  println(r)

  }


  test("testGenEntry") {
    val spark = sqlContext.sparkSession
    import spark.implicits._

    var arr1 = Seq[Double] (1.0, 2.0, Double.NaN, Double.NaN, 3.0, null.asInstanceOf[Double])
    var df = sc.parallelize(arr1).toDF("TestFeatureDouble")

    var dataframes = List(NamedDataFrame(name = "testDataSet1", df))
    var dataset:DataEntrySet = generator.toDataEntries(dataframes).head
    var entry:DataEntry = dataset.entries.head

    assert(2 === entry.missing)

    val arr2 = Seq[String] ("a","b", Float.NaN.toString, "c", null.asInstanceOf[String])
    df = sc.parallelize(arr2).toDF("TestFeatureStr")
    dataframes = List(NamedDataFrame(name = "testDataSet2", df))
    dataset   = generator.toDataEntries(dataframes).head
    entry = dataset.entries.head

    assert(2 === entry.missing)

  }
  test ("convertTimeTypes") {
    val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val ts1 = new java.sql.Timestamp(simpleDateFormat.parse("2005-02-25").getTime)
    val ts2 = new java.sql.Timestamp(simpleDateFormat.parse("2006-02-25").getTime)

    val spark = sqlContext.sparkSession
    import org.apache.spark.sql.functions._
    import spark.implicits._

    var arr = Seq[String]("2005-02-25", "2006-02-25")
    var df = sc.parallelize(arr).toDF("TestFeatureDate").select(to_date($"TestFeatureDate"))
    var dataframes = List(NamedDataFrame(name = "testDataSet1", df))
    var dataset:DataEntrySet = generator.toDataEntries(dataframes).head
    var entry:DataEntry = dataset.entries.head
    val vals = entry.values.collect().map(r => r.getAs[Long](0))

    assert(Array(1109318400000L, 1140854400000L) === vals)


    import java.time.{LocalDate, Month}

    val startDate = LocalDate.of(2008, Month.JANUARY, 1)
    val endDate = LocalDate.of(2009, Month.JANUARY, 1)
    val numberOfDays = ChronoUnit.DAYS.between(startDate, endDate)
    var arr1 = Seq[Long](numberOfDays*24*60*60*1000*1000)
    var df1 = sc.parallelize(arr1).toDF("TestFeatureDate")
    var dataframes1 = List(NamedDataFrame(name = "testDataSet1", df1))
    var dataset1:DataEntrySet = generator.toDataEntries(dataframes1).head
    var entry1:DataEntry = dataset1.entries.head
    val vals1 = entry1.values.collect().map(r => r.getAs[Long](0))

    assert(vals1.head === 31622400000000L)
  }


  test("convertDataType") {
    assert(FeatureNameStatistics.Type.INT === generator.convertDataType("Integer"))
    //Boolean and time types treated as int
    assert(FeatureNameStatistics.Type.INT === generator.convertDataType("Short"))
    assert(FeatureNameStatistics.Type.INT === generator.convertDataType("Long"))
    assert(FeatureNameStatistics.Type.INT === generator.convertDataType("java.math.BigDecimal"))
    assert(FeatureNameStatistics.Type.INT === generator.convertDataType("Boolean"))
    assert(FeatureNameStatistics.Type.INT === generator.convertDataType("Date"))
    assert(FeatureNameStatistics.Type.INT === generator.convertDataType("java.util.Date"))
    assert(FeatureNameStatistics.Type.INT === generator.convertDataType("java.sql.Date"))
    assert(FeatureNameStatistics.Type.INT === generator.convertDataType("java.sql.Timestamp"))
    assert(FeatureNameStatistics.Type.FLOAT === generator.convertDataType("Double"))
    assert(FeatureNameStatistics.Type.FLOAT === generator.convertDataType("Float"))
    assert(FeatureNameStatistics.Type.STRING === generator.convertDataType("String"))
    // Unsupported types treated as string for now
    assert(FeatureNameStatistics.Type.STRING === generator.convertDataType("Unit"))
  }


  test("testGetDatasetsProtoSequenceExampleHistogram") {
    val sp= sqlContext.sparkSession
    import sp.implicits._
    var df = sc.parallelize(Seq(1,2,2,3)).toDF("featureInt")
    var countDF = sc.parallelize(Seq (1, 2, 1)).toDF("counts")
    var featLensDF = sc.parallelize(Seq (1, 2, 1)).toDF("feat_lens")

    val entry: DataEntry = DataEntry( featureName = "featureInt",
                                      `type` = FeatureNameStatistics.Type.INT,
                                      values = df,
                                      counts = countDF,
                                      missing = 0,
                                      feat_lens = Some(featLensDF))

    var dataset:DataEntrySet = DataEntrySet(name ="testDataset",size=3, entries = Array(entry))
    val p = generator.genDatasetFeatureStats(List(dataset))
    val hist = p.datasets.head.features.head.getNumStats.getCommonStats.getFeatureListLengthHistogram
    val hist2 = p.datasets.head.features.head.getNumStats.getCommonStats.getNumValuesHistogram

    val buckets = hist.buckets
    assert(QUANTILES === hist.`type`)
    assert(10 === buckets.length)
    assert(1 === buckets.head.lowValue)
    assert(1 === buckets.head.highValue)
    assert(.3 === buckets.head.sampleCount)
    assert(1.8 === buckets(9).lowValue)
    assert(2 === buckets(9).highValue)
    assert(.3 === buckets(9).sampleCount)


  }


  test("testGetDatasetsProtoWithWhitelist") {

    val sp= sqlContext.sparkSession
    import sp.implicits._
    var df1 = sc.parallelize(Seq(1,2,3)).toDF("testFeature")
    var countDF1 = sc.parallelize(Seq (1, 1, 1)).toDF("counts")
    val entry1: DataEntry = DataEntry( featureName = "testFeature",
      `type` = FeatureNameStatistics.Type.INT,
      values = df1,
      counts = countDF1,
      missing = 0 )

    var df2 = sc.parallelize(Seq(5,6)).toDF("ignoreFeature")
    var countDF2 = sc.parallelize(Seq (1, 1)).toDF("counts")
    val entry2: DataEntry = DataEntry( featureName = "ignoreFeature",
      `type` = FeatureNameStatistics.Type.INT,
      values = df2,
      counts = countDF2,
      missing = 1 )


    var dataset:DataEntrySet = DataEntrySet(name ="testDataset",size=3, entries = Array(entry1,entry2))
    val p = generator.genDatasetFeatureStats(List(dataset), Set("testFeature"))
    assert(1 === p.datasets.length)
    val testData = p.datasets.head

    assert("testDataset" === testData.name)
    assert(3 === testData.numExamples)
    testData.features.foreach {f =>
      println("feauture name = "+ f.name)
    }
    assert(1 === testData.features.length)
    val numfeat = testData.features.head
    assert("testFeature" === numfeat.name)
    assert(1 === numfeat.getNumStats.min)

  }

  test("GetDatasetsProtoWithMaxHistigramLevelsCount") {
    val spark = sqlContext.sparkSession
    import spark.implicits._

    val data = Seq[String]("hi", "good", "hi","hi","a", "a")
    val df :DataFrame= sqlContext.sparkContext.parallelize(data).toDF("TestFeatureString")
    val dataframes = List(NamedDataFrame(name = "testDataSet", df))
//    # Getting proto from ProtoFromDataFrames instead of GetDatasetsProto
//    # directly to avoid any hand written values ex: size of dataset.
    val p = generator.protoFromDataFrames(dataframes, histgmCatLevelsCount=Some(2))


    assert(1 === p.datasets.size)
    val testData = p.datasets.head
    assert("testDataSet" === testData.name)
    assert(6 === testData.numExamples)
    assert(1 === testData.features.size)
    val numfeat = testData.features.head
    assert("TestFeatureString" === numfeat.name)
    val topValues = numfeat.getStringStats.topValues
    
    assert(3 === topValues.head.frequency)
    assert("hi" === topValues.head.value)

    assert(3 === numfeat.getStringStats.unique)
    assert(2 === numfeat.getStringStats.avgLength)

    val rank_hist = numfeat.getStringStats.rankHistogram
    assert(rank_hist.nonEmpty)

    val buckets = rank_hist.get.buckets
    assert(2 === buckets.size)
    assert("hi" === buckets.head.label)
    assert(3 === buckets.head.sampleCount)
    assert("a" === buckets(1).label)
    assert(2 === buckets(1).sampleCount)

  }


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

  def writeToFile(fileName:String, content:String): Unit = {
    import java.nio.charset.StandardCharsets
    import java.nio.file.{Files, Paths}
    Files.write(Paths.get(fileName), content.getBytes(StandardCharsets.UTF_8))
  }
}
