package features.stats.spark

import featureStatistics.feature_statistics.FeatureNameStatistics
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
    datadf.write.mode(SaveMode.Overwrite).format("tfrecords").option("recordType", TFRecordType.Example.name).save(path)

    //loaded back to test
    val df = spark.read.format("tfrecords").option("recordType", "Example").schema(datadf.schema).load(path)

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


}
