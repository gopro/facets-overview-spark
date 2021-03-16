// Copyright © 2018 GoPro, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// ==============================================================================
//

package features.stats.spark

import java.time.temporal.ChronoUnit

import featureStatistics.feature_statistics.FeatureNameStatistics
import featureStatistics.feature_statistics.Histogram.HistogramType.QUANTILES
import org.apache.spark.sql.DataFrame

class FeatureStatsGeneratorTest extends StatsGeneratorTestBase {


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
    val testData = proto.datasets.head
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
    val r = toJson(proto)
  //  println(r)

  }


  test("testGenEntry") {
    import spark.implicits._
    val sc = spark.sparkContext
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

    import java.util.TimeZone
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

    import java.time.{LocalDateTime, ZoneOffset}
    val ts1 = LocalDateTime.of(2005, 2, 25, 0, 0).toInstant(ZoneOffset.UTC).getEpochSecond
    val ts2 = LocalDateTime.of(2006, 2, 25, 0, 0).toInstant(ZoneOffset.UTC).getEpochSecond

    import org.apache.spark.sql.functions._
    import spark.implicits._

    val sc = spark.sparkContext
    var arr = Seq[String]("2005-02-25", "2006-02-25")
    var df = sc.parallelize(arr).toDF("TestFeatureDate").select(to_date($"TestFeatureDate"))
    var dataframes = List(NamedDataFrame(name = "testDataSet1", df))
    var dataset:DataEntrySet = generator.toDataEntries(dataframes).head
    assert(dataset.entries.head.`type`.isInt === true)

    var entry:DataEntry = dataset.entries.head
    val vals = entry.values.collect().map(r => r.getAs[Long](0))

    assert(Array(ts1*1000, ts2*1000) === vals)

    import java.time.{LocalDate, Month}

    val startDate = LocalDate.of(2008, Month.JANUARY, 1)
    val endDate = LocalDate.of(2009, Month.JANUARY, 1)
    val numberOfDays = ChronoUnit.DAYS.between(startDate, endDate)
    var arr1 = Seq[Long](numberOfDays*24*60*60*1000*1000)
    var df1 = spark.sparkContext.parallelize(arr1).toDF("TestFeatureDate")
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
    import spark.implicits._
    val sc = spark.sparkContext
    var df = sc.parallelize(Seq(1,2,2,3)).toDF("featureInt")
    var countDF = sc.parallelize(Seq (1, 2, 1)).toDF("counts")
    var featLensDF = sc.parallelize(Seq (1, 2, 1)).toDF("feat_lens")

    val entry: DataEntry = DataEntry( featureName = "featureInt",
                                      `type` = FeatureNameStatistics.Type.INT,
                                      values = df,
                                      counts = countDF,
                                      missing = 0,
                                      featLens = Some(featLensDF))

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
    import spark.implicits._
    val sc = spark.sparkContext
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
     import spark.implicits._

    val data = Seq[String]("hi", "good", "hi","hi","a", "a")
    val df :DataFrame= spark.sparkContext.parallelize(data).toDF("TestFeatureString")
    val dataframes = List(NamedDataFrame(name = "testDataSet", df))
//    # Getting proto from ProtoFromDataFrames instead of GetDatasetsProto
//    # directly to avoid any hand written values ex: size of dataset.
    val p = generator.protoFromDataFrames(dataframes, catHistgmLevel=Some(2))


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

}
