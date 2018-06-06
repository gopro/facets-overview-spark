package features.stats.spark

import featureStatistics.feature_statistics.FeatureNameStatistics
import featureStatistics.feature_statistics.FeatureNameStatistics.{Type => ProtoDataType}
import featureStatistics.feature_statistics.Histogram.HistogramType.{QUANTILES, STANDARD}
import org.apache.spark.sql.SaveMode
/**
  * # ==============================================================================
  * # Licensed under the Apache License, Version 2.0 (the "License");
  * # you may not use this file except in compliance with the License.
  * # You may obtain a copy of the License at
  * #
  * #     http://www.apache.org/licenses/LICENSE-2.0
  * #
  * # Unless required by applicable law or agreed to in writing, software
  * # distributed under the License is distributed on an "AS IS" BASIS,
  * # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * # See the License for the specific language governing permissions and
  * # limitations under the License.
  * # ==============================================================================
  */


class TensorStatsGeneratorTest extends StatsGeneratorTestBase {

  
  test("GetProtoNums") {

    val path = "target/example-test.tfrecord"

    val data = (0 until 50).map(i => (i, 0))
    val features = Array("num", "other")
    val spark = sqlContext.sparkSession
    import spark.implicits._
    val datadf = spark.sparkContext.parallelize(data).toDF(features: _*)
    //save it to make tensorflow record
    TFRecordHelper.writerTFRecords(datadf, path,  TFRecordType.Example, Some(SaveMode.Overwrite))

    //loaded back to test
    val df =  TFRecordHelper.loadTFRecords(spark, path,  TFRecordType.Example, Some(datadf.schema))

    //generate datastats
    val dataframes = List(NamedDataFrame(name = "test", df))
    val p = generator.protoFromDataFrames(dataframes)

    assert(1 === p.datasets.length)
    val testData = p.datasets.head
    assert("test" === testData.name)
    assert(50 === testData.numExamples)
    //assert(51 === testData.numExamples) google code

    val numfeat = if (testData.features.head.name == "num") testData.features.head else testData.features.tail.head
    assert("num" === numfeat.name)
    assert(FeatureNameStatistics.Type.INT === numfeat.`type`)
    assert(0 === numfeat.getNumStats.min)
    assert(49 === numfeat.getNumStats.max)
    assert(24.5 === numfeat.getNumStats.mean)
    //assert(24.5 === numfeat.getNumStats.median) gogole code
    assert(24.0 === numfeat.getNumStats.median)
    assert(1 === numfeat.getNumStats.numZeros)
    assert(Math.abs(14.430869689 - numfeat.getNumStats.stdDev) <= 1e-4)
    //    assert(1 === numfeat.getNumStats.getCommonStats.numMissing) google code
    assert(0 === numfeat.getNumStats.getCommonStats.numMissing)
    assert(50 === numfeat.getNumStats.getCommonStats.numNonMissing)
    assert(1 === numfeat.getNumStats.getCommonStats.minNumValues)
    assert(1 === numfeat.getNumStats.getCommonStats.maxNumValues)
    assert(Math.abs(1 - numfeat.getNumStats.getCommonStats.avgNumValues) <= 1e-4)

    val histgram = numfeat.getNumStats.getCommonStats.getNumValuesHistogram
    val buckets = histgram.buckets

    import featureStatistics.feature_statistics.{Histogram => FSHistogram}

    assert(FSHistogram.HistogramType.QUANTILES ===  histgram.`type`)
    assert(10 === buckets.length)
    assert(1 === buckets.head.lowValue)
    assert(1 === buckets.head.highValue)
    assert(5 === buckets.head.sampleCount)
    assert(1 === buckets(9).lowValue)
    assert(1 === buckets(9).highValue)
    assert(5 === buckets(9).sampleCount)

    assert(2 === numfeat.getNumStats.histograms.length)
    val histgram2 = numfeat.getNumStats.histograms.head
    val buckets2 = histgram2.buckets
    assert(FSHistogram.HistogramType.STANDARD ===  histgram2.`type`)

    assert(10 === buckets2.length)
    assert(0 === buckets2.head.lowValue)
    assert(4.9 === buckets2.head.highValue)
    assert(5 === buckets2.head.sampleCount)
    assert(Math.abs(44.1 - buckets2(9).lowValue) < 1e-2)
    assert(49 === buckets2(9).highValue)
    assert(5 === buckets2(9).sampleCount)


    val histogram3 = numfeat.getNumStats.histograms(1)
    val buckets3 = histogram3.buckets
    assert(FSHistogram.HistogramType.QUANTILES ===  histogram3.`type`)

    assert(10 === buckets3.length)
    assert(0 === buckets3.head.lowValue)
    assert(4.9 === buckets3.head.highValue)
    assert(5 === buckets3.head.sampleCount)
    assert(Math.abs(44.1 - buckets3(9).lowValue) < 1e-2)
    assert(49 === buckets3(9).highValue)
    assert(5 === buckets3(9).sampleCount)
  }


  test("testQuantiles") {
    val data1 = (0 until 50)
    val data2 = (0 until 50).map(i => 100)

    val features = Array("num")
    val spark = sqlContext.sparkSession
    import spark.implicits._
    val datadf1 = spark.sparkContext.parallelize(data1++data2).toDF(features: _*)

    //generate datastats
    val dataframes = List(NamedDataFrame(name = "test", datadf1) )
    val p = generator.protoFromDataFrames(dataframes)

    val numfeat = p.datasets.head.features.head
    assert(2 ===  numfeat.getNumStats.histograms.length)
    assert(QUANTILES ===  numfeat.getNumStats.histograms(1).`type`)
    val buckets = numfeat.getNumStats.histograms(1).buckets
    assert(10 ===  buckets.length)
    assert(0 ===  buckets.head.lowValue)
    assert(9.9 ===  buckets.head.highValue)
    assert(10 ===  buckets.head.sampleCount)
    assert(100 ===  buckets(9).lowValue)
    assert(100 ===  buckets(9).highValue)
    assert(10 ===  buckets(9).sampleCount)


  }

  test("testInfinityAndNan") {
    val data1 = (0 until 50).map(i => 1.0F*i)
    val data2 = Array(Float.PositiveInfinity, Float.NegativeInfinity, Float.NaN)

    val features = Array("num")
    val spark = sqlContext.sparkSession
    import spark.implicits._
    val datadf1 = spark.sparkContext.parallelize(data1++data2).toDF(features: _*)

    //generate datastats
    val dataframes = List(NamedDataFrame(name = "test", datadf1) )
    val p = generator.protoFromDataFrames(dataframes)

    println(toJson(p))

    val numfeat = p.datasets.head.features.head
    assert(ProtoDataType.FLOAT ===  numfeat.`type`)

    assert(Double.NaN.equals(numfeat.getNumStats.min))
    assert(Double.NaN.equals(numfeat.getNumStats.max))
    assert(Double.NaN.equals(numfeat.getNumStats.mean))
    assert(Double.NaN.equals(numfeat.getNumStats.median))
    assert(Double.NaN .equals(numfeat.getNumStats.stdDev))
    assert(1 ===  numfeat.getNumStats.numZeros)
    assert(1 ===  numfeat.getNumStats.commonStats.get.numMissing)
    assert(52 ===  numfeat.getNumStats.commonStats.get.numNonMissing) //google code : assert(53, numfeat.num_stats.common_stats.num_non_missing)
    val hist = numfeat.getNumStats.histograms.head
    val buckets = hist.buckets
    assert(STANDARD ===  hist.`type`)
    assert(1 ===  hist.numNan)
    assert(10 ===  buckets.length)
    assert(Double.NegativeInfinity ===  buckets.head.lowValue)
    assert(4.9 ===  buckets.head.highValue)
    assert(6 ===  buckets.head.sampleCount)
    assert(44.1 ===  buckets(9).lowValue)
    assert(Double.PositiveInfinity ===  buckets(9).highValue)
    assert(6 ===  buckets(9).sampleCount)
  }

  test("testInfinitysOnly") {

    val data1 = Array(Float.NegativeInfinity, Float.PositiveInfinity)

    val features = Array("num")
    val spark = sqlContext.sparkSession
    import spark.implicits._
    val datadf1 = spark.sparkContext.parallelize(data1).toDF(features: _*)

    //generate datastats
    val dataframes = List(NamedDataFrame(name = "test", datadf1) )
    val p = generator.protoFromDataFrames(dataframes)

    println(toJson(p))

    val numfeat = p.datasets.head.features.head
    val hist = numfeat.getNumStats.histograms.head
    val buckets = hist.buckets

    //numpy empty array histgram return a bucket of 10 and values range from 0 to 0.9
    //we did not follow this default behavior

    assert(STANDARD ===  hist.`type`)
//    assert(10 ===  buckets.length)
//    assert(Double.NegativeInfinity ===  buckets.head.lowValue)
//    assert(0.1 ===  buckets.head.highValue)
//    assert(1 ===  buckets.head.sampleCount)
//    assert(0.9 ===  buckets(9).lowValue)
//    assert(Double.PositiveInfinity ===  buckets(9).highValue)
//    assert(1 ===  buckets(9).sampleCount)

        assert(1 ===  buckets.length)
        assert(Double.NegativeInfinity ===  buckets.head.lowValue)
        assert(Double.PositiveInfinity  ===  buckets.head.highValue)
        assert(2 ===  buckets.head.sampleCount) //google code : sample code = 1, I added both neg inf + pos inf counts

  }


  test("GetProtoMultipleDatasets") {
    //     Tests converting multiple datsets into the feature stats proto
    //     including ensuring feature order is consistent in the protos.
    val spark = sqlContext.sparkSession
    import spark.implicits._

    val data1 = (0 until 2).map(i => ("one", 0))
    val features1 = Array("str", "num")
    val data2 = Seq((1, "two"))
    val features2 = Array("num", "str")
    
    val df1 = spark.sparkContext.parallelize(data1).toDF(features1: _*)
    val df2 = spark.sparkContext.parallelize(data2).toDF(features2: _*)
    df1.show()
    df2.show()

    //generate datastats
    val dataframes = List(NamedDataFrame(name = "test1", df1), NamedDataFrame(name="test2", df2))
    val p = generator.protoFromDataFrames(dataframes)


    assert(2 === p.datasets.length)
    val testData1 = p.datasets.head 
    assert("test1" === testData1.name)
    assert(2 ===testData1.numExamples)
    val numFeatIndex1 = if (testData1.features.head.name == "num") 0 else 1
    assert(0 === testData1.features(numFeatIndex1).getNumStats.max)


    val testData2 = p.datasets(1)
    val numFeatIndex2 = if (testData2.features.head.name == "num") 0 else 1
    assert("test2" === testData2.name)
    assert(1 === testData2.numExamples)
    assert(1 === testData2.features(numFeatIndex2).getNumStats.max)

  }

  test("GetProtoStrings") {


    val data1 = Array.fill(2)("hello")
    val data2 = Array.fill(3)("hi")
    val data3 = Array("hey")
    val data  = data1 ++ data2 ++ data3

    val features = Array("str")
    val spark = sqlContext.sparkSession

    import spark.implicits._
    val datadf1 = spark.sparkContext.parallelize(data).toDF(features: _*)

    //generate datastats
    val dataframes = List(NamedDataFrame(name = "test", datadf1) )
    val p = generator.protoFromDataFrames(dataframes)

    println(toJson(p))
    val testData = p.datasets.head

    assert(1 === p.datasets.length)
    assert("test" === testData.name)
    assert(6 === testData.numExamples)
    val strfeat = testData.features.head
    assert("str" === strfeat.name)
    assert(ProtoDataType.STRING === strfeat.`type`)
    assert(3 === strfeat.getStringStats.unique)
    assert(Math.abs(19 / 6.0 - strfeat.getStringStats.avgLength) <= 1E-4)
    assert(0 ===strfeat.getStringStats.commonStats.get.numMissing)
    assert(6 ===strfeat.getStringStats.commonStats.get.numNonMissing)
    assert(1 ===strfeat.getStringStats.commonStats.get.minNumValues)
    assert(1 ===strfeat.getStringStats.commonStats.get.maxNumValues)
    assert(1 ===strfeat.getStringStats.commonStats.get.avgNumValues)

    val hist = strfeat.getStringStats.commonStats.get.getNumValuesHistogram
    val buckets = hist.buckets
    assert(QUANTILES ===  hist.`type`)
    assert(buckets.length === 10)
    assert(buckets.head.lowValue === 1)
    assert(buckets.head.highValue === 1)
    assert(buckets.head.sampleCount === .6)
    assert(buckets(9).lowValue === 1)
    assert(buckets(9).highValue === 1)
    assert(buckets(9).sampleCount === .6)

    assert(strfeat.getStringStats.topValues.length === 2)
    assert(strfeat.getStringStats.topValues.head.frequency === 3)
    assert(strfeat.getStringStats.topValues.head.value === "hi")
    assert(strfeat.getStringStats.topValues(1).frequency === 2)
    assert(strfeat.getStringStats.topValues(1).value === "hello")

    assert(strfeat.getStringStats.rankHistogram.nonEmpty)

    val rankbuckets = strfeat.getStringStats.rankHistogram.get.buckets
    assert(rankbuckets.length === 3)
    assert(rankbuckets.head.lowRank === 0)
    assert(rankbuckets.head.highRank === 0)
    assert(rankbuckets.head.sampleCount === 3)
    assert(rankbuckets.head.label === "hi")
    assert(rankbuckets(2).lowRank === 2)
    assert(rankbuckets(2).highRank === 2)
    assert(rankbuckets(2).sampleCount === 1)
    assert(rankbuckets(2).label === "hey")

  }


  test("TFExamplewithMissingValues") {

    val data  = Seq(null, "test" )
    val features = Array("str")
    val spark = sqlContext.sparkSession

    import spark.implicits._
    val datadf1 = spark.sparkContext.parallelize(data).toDF(features: _*)
    val dataframes = List(NamedDataFrame(name = "test", datadf1) )
    val p = generator.protoFromDataFrames(dataframes)

    val strfeat = p.datasets.head.features.head
    val strfeatStats = strfeat.getStringStats
    assert(strfeatStats.commonStats.get.numMissing == 1)
    assert(strfeat.`type` == ProtoDataType.STRING)


  }

  test("TFExampleSequenceContxt") {
    val data  = (0 until 50 )
    val features = Array("num")
    val spark = sqlContext.sparkSession

    import spark.implicits._
    val datadf1 = spark.sparkContext.parallelize(data).toDF(features: _*)
    val dataframes = List(NamedDataFrame(name = "test", datadf1) )
    val p = generator.protoFromDataFrames(dataframes)

    println("json=" + toJson(p))

    assert(p.datasets.head.numExamples ==50)
    val numfeat      = p.datasets.head.features.head
    val numfeatStats = numfeat.getNumStats
    assert(numfeatStats.commonStats.get.numMissing == 0)
    assert(numfeat.`type` == ProtoDataType.INT)

    //note can not use getFeatureListLengthHistogram as it will return default hist when not defined.
    assert(numfeat.getNumStats.commonStats.get.featureListLengthHistogram === None)

  }

  private val seqdata= (0 until 50).toArray.toList
  test("ExampleSequenceFeatureList") {
    val data  = Array(Array(seqdata))
    val features = Array("num")
    val spark = sqlContext.sparkSession

    import spark.implicits._
    val datadf1 = spark.sparkContext.parallelize(data).toDF(features: _*)
    val dataframes = List(NamedDataFrame(name = "test", datadf1) )
    val p = generator.protoFromDataFrames(dataframes)
    println("json=" + toJson(p))

    assert(p.datasets.head.numExamples ==1)
    val numfeat      = p.datasets.head.features.head
    assert(numfeat.getNumStats.commonStats.get.featureListLengthHistogram.isDefined)

  }

  test("ExampleSequenceFeatureWithValueList") {
    val data1  = Seq(Seq(seqdata), Seq(seqdata))
    val features = Array("num")
    val spark = sqlContext.sparkSession

    import spark.implicits._
    val datadf1 = spark.sparkContext.parallelize(data1).toDF(features: _*)
    datadf1.show()


    val dataframes = List(NamedDataFrame(name = "test", datadf1) )
    val p = generator.protoFromDataFrames(dataframes)
    println("json=" + toJson(p))

    assert(p.datasets.head.numExamples ==2)
    val numfeat      = p.datasets.head.features.head
    assert(numfeat.getNumStats.commonStats.get.featureListLengthHistogram.isDefined)

  }


  test("ExampleSequenceWithMultipleFeatureValueList") {

    val featData1 = (0 until 25).map( j =>  Array(j) ).toArray.toList
    val featData2 = (0 until 25).map( j =>  Array(25 + j) ).toArray.toList

    val features = Array("num")
    val spark = sqlContext.sparkSession

    import spark.implicits._
    val datadf1 = spark.sparkContext.parallelize(Seq(featData1)++ Seq(featData2)).toDF(features: _*)
    datadf1.show()

    DataFrameUtils.flattenDataFrame(datadf1, recursive = false).show()

    val dataframes = List(NamedDataFrame(name = "test", datadf1) )
    val p = generator.protoFromDataFrames(dataframes)
    println("json=" + toJson(p))

    assert(p.datasets.head.numExamples ==2)
    val numfeat  = p.datasets.head.features.head
    assert(numfeat.getNumStats.commonStats.get.featureListLengthHistogram.isDefined)

    val featListHist = numfeat.getNumStats.commonStats.get.featureListLengthHistogram

    assert(featListHist.isDefined)

    val featListHistBuckets = featListHist.get.buckets
    assert(featListHistBuckets.head.lowValue === 25)
    assert(featListHistBuckets.head.highValue === 25)
  }


    ignore ("SequenceExample1") {

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

  }




}
