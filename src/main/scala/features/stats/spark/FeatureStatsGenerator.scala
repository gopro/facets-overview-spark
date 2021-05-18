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


package features.stats.spark
import featureStatistics.feature_statistics.FeatureNameStatistics.Type._
import featureStatistics.feature_statistics.FeatureNameStatistics.{Type => ProtoDataType}
import featureStatistics.feature_statistics.StringStatistics.FreqAndValue
import featureStatistics.feature_statistics.{Histogram => FSHistogram, _}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._

import scala.collection.mutable

/**
  *
  * Class for generating the feature statistics.
  *
  * The feature statistics is used as input for the Overview visualization.
  *
  * This class initial implementation reference the GOOGLE FACETS facet_overview python
  * (https://github.com/PAIR-code/facets/tree/master/facets_overview/python) implementation logic
  *
  * The Spark DataFrame is used instead of using Python numpy and pandas.
  *
  * The facet overview protobuf defines the following structure:
  *
  *  Dataset -- example training or test data sets, each dataset has DatasetFeatureStatistics which describes the dataset's statistics
  *
  *  DatasetFeatureStatistics -- contains lists of metadata (name of dataset, number of sample, and Seq[FeatureNameStatistics])
  *
  *  FeatureNameStatistics -- Each FeatureNameStatistics is feature stats for the given dataset,
  *                           which includes feature name and data type and one of (NumericStatistics, StringStatistics or binary stats)
  *
  *   NumericStatistics Contains:
  *     ** CommonStatistics -- (std_dev,mean, median,min, max, histogram)
  *     ** WeightedNumericStatistics weighted_numeric_stats
  *
  *  StringStatistics Contains :
  *     ** CommonStatistics common_stats = 1;
          (unique values, FreqAndValue {value, frequency}, FreqAndValue top_values = 3,avg_length = 4, rank_histogram )
        ** weighted_string_stats;
  *
  *
  * Special Notes: For Tensorflow Data Structure TFRecords
  * TFRecords Structure samples
  * https://medium.com/mostly-ai/tensorflow-records-what-they-are-and-how-to-use-them-c46bc4bbb564
  *
  * If the data are from CSV, JSON etc format, we can convert the data to DataFrame (pandas or Spark), for Tensorflow
  * TFRecords, Python implementation directly parse the TFRecord, we are using Tensorflow-spark-connector to parse it.
  * The TFRecords are essentially TF Example or TF SequenceExample.
  *  ** TF Example is set of features. each feature contains single value or list of values.
  *
  *  ** TF sequenceExample, the data consists context data and data. The data contains feature list.
  *     each feature list can have multiple features and each feature can have one value or array of values.
  *
  *  ** Mapping to data frame, one SequenceExample can be one DataFrame.
  *     one feature List is one column and the column value is Array of Array of values
  *
  * Feature Lists can be related, for example
  *
  * Movie Names    = [["The Shawshank Redemption"],["Fight Club"]]  --- Array of Array of movie names
  * Movie Actors   = [["Tim Robbins","Morgan Freeman"], ["Brad Pitt","Edward Norton","Helena Bonham Carter"]] --array of array of actor names
  * Movie Ratings  = [[9.0], [9.7]] --array of array of ratings
  *
  * the the first array of actors : ["Tim Robbins","Morgan Freeman"] are in first array of movies: ["The Shawshank Redemption"],["Fight Club"] with ratings [9.0]
  * the the 2nd array of actors : ["Brad Pitt","Edward Norton","Helena Bonham Carter"] are in 2nd array of movies: ["Fight Club"] with ratings [9.7]
  *
  */


object FeatureStatsGenerator {

  val dateFormat = "yyyy-MM-dd'T'HH:mm:ssZ"
  private[features] case class NonZeroInfinite(isNan: Int, isZero: Int, isPosInfinite: Int, isNegInfinite: Int)


  private[features]  def isFiniteFun(x : Any): Boolean = {
    val value = x.toString.toDouble //todo fix
    !value.isNaN && !value.isInfinite
  }
  private[features]  def isNaNFun(value : Any): Int = {

    val isNull = value == null
    val isNan = value match {
      case v: Float => v.equals(Float.NaN)
      case v: Double => v.equals(Double.NaN)
      case _ => false
    }
    if (isNull || isNan) 1 else 0
  }
  private[features]  def isNotNaNFun(value : Any): Int = if (isNaNFun(value) == 1) 0 else 1
  private[features]  def nonZeroOrEmptyValue(value: Any) : Int = {
    value match {
      case x:String  => if (x.nonEmpty) 1 else 0
      case y:Double  => if (y != 0) 1 else 0
      case y:Float  => if (y != 0) 1 else 0
      case y:Int   => if (y != 0) 1 else 0
      case y:Long   => if (y != 0) 1 else 0
      case _ => 1
    }
  }
  private[features]  def checkZeroNanInfiniteFun(x : AnyVal) : NonZeroInfinite = {

    val value = x.toString.toDouble

    val (isNan, isPosInfi, isNegInfi, isZero) = {
      if (value.isNaN) {
        (1, 0, 0, 0)
      }
      else {
        if (value.isPosInfinity) {
          (0, 1, 0, 0)
        }
        else if (value.isNegInfinity) {
          (0, 0, 1, 0)
        }
        else if (value.abs < 1e-10) {
          (0,0,0,1)
        }
        else {
          (0,0,0,0)
        }
      }
    }

    NonZeroInfinite(isNan = isNan, isZero = isZero, isPosInfinite = isPosInfi, isNegInfinite = isNegInfi)
  }
  private[features]  def nonZeroOrEmpty: UserDefinedFunction= udf((value : Any) =>nonZeroOrEmptyValue(value))
  private[features]  val checkZeroInfinite : UserDefinedFunction= udf((value : Double) =>checkZeroNanInfiniteFun(value))

}


class FeatureStatsGenerator(datasetProto: DatasetFeatureStatisticsList) {

  import FeatureStatsGenerator._

  /**
    * Creates a feature statistics proto from a set of pandas data frames.
    *
    * @param dataFrames         A list of dicts describing tables for each dataset for the proto.
    *                           Each entry contains a 'table' field of the dataframe of the data and a 'name' field
    *                           to identify the dataset in the proto.
    * @param catHistgmLevel int, controls the maximum number of levels to display in histograms
    *                           for categorical features. Useful to prevent codes/IDs features
    *                           from bloating the stats object. Defaults to 20. Set None for no control
    * @return The feature statistics proto for the provided tables.
    */
  def protoFromDataFrames(dataFrames     : List[NamedDataFrame],
                          features       : Set[String] = Set.empty[String],
                          catHistgmLevel : Option[Int] = Some(20)): DatasetFeatureStatisticsList = {

    genDatasetFeatureStats(toDataEntries( dataFrames), features, catHistgmLevel)

  }

  private[features] def toDataEntries(dataFrames: List[NamedDataFrame]) : List[DataEntrySet] = {

    import DataFrameUtils._

    val datasets: List[DataEntrySet] = dataFrames.map { ndf =>
      val df = ndf.data
      val sp  = df.sqlContext.sparkSession
      import org.apache.spark.sql.functions._

      val dataEntries = df.schema.fields map { f =>
        val colName = f.name
        val origDataType = f.dataType

        val columnDF = df.select(df(f.name))
        val featureDF = if (isNestedArrayType(f)) flattenDataFrame(columnDF, recursive = false) else columnDF
        val flattenDF = flattenDataFrame(featureDF)
        val convertedDF = convertDateDataFrame(flattenDF.select(f.name))
        val protoTypeName = convertDataType(convertedDF.schema.head.dataType.typeName)

        //Remove all null and nan values and count how many were removed.
        //also remove "NaN" strings
        val filteredFlattenedDF = convertedDF.na.drop().filter(!convertedDF(colName).isNaN)

        val (featLens, counts, missingCount) =
          if (isNestedArrayType(f)) {
            val sequenceLengthDF  = df.select(size(df(f.name)))
            val valueLengthDF  :DataFrame   = featureDF.select(size(featureDF(f.name)))
            //empty value list are considered missing
            val missingCount      = valueLengthDF.filter(r=> r.getAs[Int](0) == 0)
            (Some(sequenceLengthDF), valueLengthDF, missingCount.count())
          }
          else{
            val origSize = flattenDF.count()
            //get RowCount DataFrame
            val rowCountDF = toRowCountDF(featureDF)
            val missingCount = origSize - filteredFlattenedDF.count()
            (None, rowCountDF, missingCount)
          }

        DataEntry(featureName=f.name,
          `type` = protoTypeName,
          values = convertedDF,
          counts = counts,
          missing = missingCount,
          featLens = featLens
        )
      }

      DataEntrySet(name = ndf.name, size = df.count, dataEntries)

    }
    datasets
  }

  /**
    * Generates the feature stats proto from dictionaries of feature values.
    *
    * @param datasets             An array of DataEntrySet, one per dataset, each one containing:
    *- 'entries': The dictionary of features in the dataset from the parsed examples.
    *- 'size': The number of examples parsed for the dataset.
    *- 'name': The name of the dataset.
    *
    * @param features A list of strings that is a whitelist of feature names to create
                      feature statistics for. If set to Empty then all features in the dataset
                      are analyzed.Defaults to Empty.
    *
    * @param catHistgmLevel controls the maximum number of
    *                             levels to display in histograms for categorical features.
    *                             Useful to prevent codes/IDs features from bloating the stats object.
    *                             Defaults to None.
    * @return
    *         The feature statistics proto for the provided datasets.
    */

  private[features] def genDatasetFeatureStats(datasets    : List[DataEntrySet],
                                               features    : Set[String] = Set.empty[String],
                                               catHistgmLevel : Option[Int] = None):DatasetFeatureStatisticsList = {

    val allDatasets = datasetProto

    val dfsList : List[DatasetFeatureStatistics]= datasets.zipWithIndex.map { a =>
      val (ds, dsIndex) = a

      def includeFeature(feature:String) : Boolean =
        if(features.isEmpty) true else features.contains(feature)

      val feat = ds.entries.filter(e => includeFeature(e.featureName))
                            .map { entry =>
        val featureType = entry.`type`
        val valueDF: DataFrame   = entry.values
        val featureName:String   = entry.featureName
        if (featureType == ProtoDataType.INT || featureType == ProtoDataType.FLOAT) {
          FeatureNameStatistics(name = entry.featureName, `type` = featureType)
                              .withNumStats(getNumericStats(entry, ds.size,dsIndex))
        }
        else {
          FeatureNameStatistics(name = entry.featureName, `type` = featureType)
                              .withStringStats(getStringStats( entry, ds.size, dsIndex, catHistgmLevel))
        }
      }

      DatasetFeatureStatistics(ds.name, numExamples = ds.size, features = feat)
    }

    dfsList.foldLeft(allDatasets){(z,dfs) => z.addDatasets(dfs)}

  }

  private[features] def getCommonStats(dsSize: Long, entry: DataEntry, numNan: Long, dsIndex:Int) : CommonStatistics = {

    val countDF= entry.counts

    val countQuantileBuckets = populateQuantilesHistogramBucket(entry.counts,dsIndex)
    val countQuantileHist = FSHistogram(numNan = numNan,
                                        buckets = countQuantileBuckets,
                                        `type` = FSHistogram.HistogramType.QUANTILES)

    val featureListLenHist :Option[FSHistogram]
            = entry.featLens.map { featLens =>
              val flQuantileBuckets = populateQuantilesHistogramBucket(featLens,dsIndex)
              FSHistogram(numNan = numNan,
                          buckets = flQuantileBuckets,
                          `type` = FSHistogram.HistogramType.QUANTILES)
            }


    import org.apache.spark.sql.functions._
    val column0 : Column = countDF(countDF.columns(0))
    val row = countDF.agg(min(column0), max(column0), mean(column0)).toDF("min", "max", "mean").collect().head
    val minValue = row.getAs[Int]("min")
    val maxValue = row.getAs[Int]("max")
    val meanValue = row.getAs[Double]("mean")

    val commStats = CommonStatistics( numNonMissing = dsSize - entry.missing,
                                      minNumValues = minValue,
                                      maxNumValues = maxValue,
                                      avgNumValues = meanValue.toFloat,
                                      numValuesHistogram = Some(countQuantileHist),
                                      featureListLengthHistogram = featureListLenHist,
                                      numMissing = entry.missing
                                    )

    commStats
  }

  private[features] def getBasicNumStats(valueDF: DataFrame, colName: String,dsIndex:Int ) : BasicNumStats = {

    val spark = valueDF.sqlContext.sparkSession
    val extraCounts: Map[String, Long] = getCounts(valueDF, colName,dsIndex)
    val valueCount : Long = extraCounts("valueCount")
    val numNan     : Long = extraCounts("nanCount")
    val numZeros   : Long = extraCounts("zeroCount")
    val numPosinf  : Long = extraCounts("posInfiCount")
    val numNeginf  : Long = extraCounts("negaInfiCount")

    //          # Remove all non-finite (including NaN) value from the numeric
    //          # valueDF in order to calculate histogram buckets/counts. The
    //          # inf valueDF will be added back to the first and last buckets.
    val filteredValueDF = valueDF.na.drop().filter(!valueDF(colName).isNaN)
                                           .filter(r => isFiniteFun(r.getAs(0)))

    val rdd : RDD[Double]= convertToDoubleRDD(filteredValueDF)

    val stddevVal = getStddev(numNan, numPosinf, numNeginf, rdd)
    val meanVal   = getMean(numNan, numPosinf, numNeginf, rdd)
    val maxVal    = getMax(numNan, numPosinf, rdd)
    val minVal    = getMinValue(numNan, numNeginf, rdd)
    val medianVal = getMedian(spark,colName, numNan, numPosinf, numNeginf, filteredValueDF)

    val bucketCount = 10
    val hist = if (rdd.isEmpty()) (Array[Double](), Array[Long]()) else rdd.histogram(bucketCount)
    val stats = BasicNumStats(colName, numCount = valueCount, numNan, numZeros, numNeginf,numPosinf,
                  stddev = stddevVal,
                  mean = meanVal,
                  min=  minVal,
                  median=medianVal,
                  max = maxVal,
                  histogram = hist
                )

    stats

  }

  private def getCounts(valueDF: DataFrame, colName: String,dsIndex:Int) :Map[String, Long] = {

    val spark = valueDF.sqlContext.sparkSession
    import spark.implicits._

    valueDF.createOrReplaceTempView(s"`${colName}_view_$dsIndex`")

    val sumDF = valueDF.mapPartitions { it =>
      it.map { row =>
        var r :AnyVal = if (row.get(0) == null) Double.NaN else row.getAs[Double](0)
        val a: NonZeroInfinite = checkZeroNanInfiniteFun(r)
        (colName, a.isZero, a.isPosInfinite, a.isNegInfinite, a.isNan, 1)
      }
    }.toDF(colName, "isZero", "isPosInfi", "isNegInfi", "isNan", "hasValue")
      .groupBy(colName)
      .sum()
      .toDF(colName, "zeroCount", "posInfiCount", "negaInfiCount", "nanCount", "valueCount")

    val countsDF = sumDF.select($"zeroCount", $"posInfiCount", $"negaInfiCount", $"nanCount", $"valueCount")
    val indexedFields = countsDF.schema.fields.zipWithIndex
    countsDF.collect.flatMap(row => indexedFields.map(f => f._1.name -> row.getAs[Long](f._2))).toMap
  }

  private def getMedian(spark: SparkSession,colName: String,numNan: Long,numPosinf: Long,numNeginf: Long, filteredValueDF: DataFrame) : Double = {

    import spark.implicits._

    if (numNan > 0)
      Double.NaN
    else {

      val minNumbers = (1L to numNeginf).map(i => Double.MinValue + i * 0.1)
      val maxNumbers = (1L to numPosinf).map(i => Double.MaxValue - i * 0.1)

      val minDF = spark.sparkContext.parallelize(minNumbers).toDF(Array(colName): _*)
      val maxDF = spark.sparkContext.parallelize(maxNumbers).toDF(Array(colName): _*)
      val newDF = minDF.union(filteredValueDF).union(maxDF)
      newDF.stat.approxQuantile(colName, Array(0.5), 10e-5).head
    }
  }

  private def getStddev(numNan: Long, numPosinf: Long, numNeginf: Long, rdd: RDD[Double]): Double = {
    if (numNan > 0 || numNeginf > 0 || numPosinf > 0) Double.NaN else rdd.stdev()
  }

  private def getMinValue(numNan: Long, numNeginf: Long, rdd: RDD[Double]) : Double = {
    if (numNan > 0) Double.NaN else {
      if (numNeginf > 0) Double.NegativeInfinity else rdd.min()
    }
  }

  private def getMax(numNan: Long, numPosinf: Long, rdd: RDD[Double]) : Double = {
    if (numNan > 0) Double.NaN else {
      if (numPosinf > 0) Double.PositiveInfinity else rdd.max()
    }
  }

  private def convertToDoubleRDD(filteredValueDF: Dataset[Row]) = {
    filteredValueDF.rdd.map { a =>
      a.get(0) match {
        case x: Double => x
        case x: Float => x.toDouble
        case x: Long => x.toDouble
        case x: Int => x.toDouble
        case x: Short => x.toDouble
      }
    }
  }

  private def getMean(numNan: Long, numPosinf: Long, numNeginf: Long, rdd: RDD[Double]) : Double = {
    if (numNan > 0) Double.NaN else {
      if (numNeginf > 0 && numPosinf > 0)
        Double.NaN
      else if (numNeginf > 0 && numPosinf == 0)
        Double.NegativeInfinity
      else if (numNeginf == 0 && numPosinf > 0)
        Double.PositiveInfinity
      else
        rdd.mean()
    }
  }

  private[features] def getBasicStringStats(valueDF: DataFrame, colName: String) : BasicStringStats = {

    val spark = valueDF.sqlContext.sparkSession
    import spark.implicits._

    val aggDF = valueDF.map(row => (colName, isNaNFun(row.getAs(0)), 1L))
                       .toDF("colName", "isNaN", "isOne")
                       .groupBy("colName")
                       .sum()
                       .toDF("colName", "nanCount", "count")

    val countsDF = aggDF.select($"nanCount", $"count")
    val result : Array[Row]= countsDF.collect

    val indexedColNames : Array[(String, Int)]= countsDF.schema.fieldNames.zipWithIndex

    val extraCounts : Map[String, Long] = result.flatMap(row => indexedColNames.map(f => f._1 -> row.getAs[Long](f._2)))
                                                .toMap

    val numCount = if (extraCounts.isEmpty) 0 else extraCounts("count")
    val numNan   = if (extraCounts.isEmpty) 0 else extraCounts("nanCount")

    BasicStringStats(colName,numCount = numCount, numNan = numNan)
  }

  private[features] def updateStdHistogramBucket(colName: String,
                                                 basicNumStats: BasicNumStats): Seq[FSHistogram.Bucket] =  {

    import FSHistogram.Bucket
    val (startValues: Array[Double], counts: Array[Long]) = basicNumStats.histogram

    //add startValues, add the infinite numbers to the 1st and last startValues
    val xs: Seq[(Long, Int)] = counts.zipWithIndex
    val ys: Seq[(Double, Double, Long)] =
      if (xs.nonEmpty)
        xs.map { a: (Long, Int) =>
          val (count, index) = a
          if (index == 0 && basicNumStats.numNeginf > 0) {
            val low = Double.NegativeInfinity
            val high = startValues(index + 1)
            val sampleCount = count + basicNumStats.numNeginf

            (low, high, sampleCount)
          } else if (index == xs.length - 1 && basicNumStats.numPosinf > 0) {
            val low = startValues(index)
            val high = Double.PositiveInfinity
            val sampleCount = count + basicNumStats.numPosinf
            (low, high, sampleCount)
          }
          else {
            val low = startValues(index)
            val high = startValues(index + 1)
            val sampleCount = count
            (low, high, sampleCount)
          }
        }
      else {

        if (basicNumStats.numNeginf > 0) {
          if (basicNumStats.numPosinf == 0)
            Seq((Double.NegativeInfinity, Double.NegativeInfinity, basicNumStats.numNeginf))
          else
            Seq((Double.NegativeInfinity, Double.PositiveInfinity, basicNumStats.numNeginf + basicNumStats.numPosinf))
        }
        else if (basicNumStats.numPosinf > 0)
          Seq((Double.PositiveInfinity, Double.PositiveInfinity, basicNumStats.numPosinf))
        else
          Seq.empty
      }

    ys.map(a => Bucket(lowValue = a._1, highValue = a._2, sampleCount = a._3))
  }


  private[features] def getNumericStats(entry: DataEntry, dsSize:Long, dsIndex :Int) : NumericStatistics = {

    val valueDF: DataFrame   = entry.values
    val featureName:String   = entry.featureName
//    val countDF : DataFrame  = entry.counts

    val basicStats  = getBasicNumStats(valueDF, featureName, dsIndex)
    val stdBuckets  = updateStdHistogramBucket(featureName, basicStats)
    val stdHist     = FSHistogram(numNan = basicStats.numNan,
                                  buckets = stdBuckets,
                                  `type` = FSHistogram.HistogramType.STANDARD)

    //val filteredValueDF = valueDF.filter(r => isFiniteFun(r.getAs(0)))
    val filteredValueDF = valueDF.na.drop().filter(!valueDF(featureName).isNaN)
                                           .filter(r => isFiniteFun(r.getAs(0)))
    val qBuckets =  if (filteredValueDF.rdd.isEmpty())
                      Seq[FSHistogram.Bucket]()
                    else
                      populateQuantilesHistogramBucket(filteredValueDF, dsIndex)

    val qHist       = FSHistogram(numNan = basicStats.numNan,
                                  buckets = qBuckets,
                                 `type` = FSHistogram.HistogramType.QUANTILES)

    val commonStats: CommonStatistics = getCommonStats(dsSize, entry, basicStats.numNan,dsIndex)

    NumericStatistics(commonStats = Some(commonStats),
                      mean        = basicStats.mean,
                      stdDev      = basicStats.stddev,
                      numZeros    = basicStats.numZeros,
                      min         = basicStats.min,
                      median      = basicStats.median,
                      max         = basicStats.max,
                      histograms  = Seq(stdHist, qHist))

  }

  // Note:spark 2.3 allow to use multiple columns in one pass for
  // one-hot encoding,bucketizer, numberdiscrizer

  private[features] def getStringStats(entry: DataEntry,
                                       dsSize : Long,
                                       dsIndex:Int,
                                       histgmCatLevelsCount: Option[Int]) : StringStatistics = {


    val valueDF: DataFrame    = entry.values
    val featureName:String    = entry.featureName
    val basicStats  = getBasicStringStats(valueDF, featureName)
    val spark = valueDF.sqlContext.sparkSession
    val commonStats:CommonStatistics  = getCommonStats(dsSize, entry, basicStats.numNan,dsIndex)

    val colDF : DataFrame  = valueDF.select(featureName)
    val avgLen  : Double   = colDF.rdd.map{a =>
                                            val s = a.getAs[String](0)
                                            if (s == null || s.isEmpty) 0 else s.length
                                          }
                                      .mean()

    val distinctCount = colDF.rdd.distinct.count()
    val ranksRdd = colDF.rdd.mapPartitions(it => it.map(r => (r.getAs[String](0),1L)))
                                                   .reduceByKey(_+_)
                                                   .sortBy(a =>  -1 * a._2)
    val ranks :Array[(String,Long)] = histgmCatLevelsCount.map(ranksRdd.take).getOrElse(ranksRdd.collect)
    val buckets = ranks.zipWithIndex.map { a =>
                    val ((cat, count), index) = a
                    var category = if (cat == null)  "null" else cat
                    RankHistogram.Bucket(lowRank = index, highRank = index, sampleCount = count, label = category)
                  }

    val top2Buckets = buckets.take(2).map {b =>FreqAndValue(value = b.label, frequency = b.sampleCount)}
    StringStatistics(Some(commonStats),
                     topValues      = top2Buckets,
                     unique         = distinctCount,
                     avgLength      = avgLen.toFloat,
                     rankHistogram  = Some(RankHistogram(buckets)))
  }


  //Quantile histogram (different from data histogram)
  private[features] def populateQuantilesHistogramBucket(valuedf: DataFrame, dsIndex:Int) : Seq[FSHistogram.Bucket]= {

      val colName = valuedf.schema.fields(0).name
      val numQuantileBuckets = 10
      val quantilesTarget: Array[Double] = (0 until numQuantileBuckets + 1).map(x => x * 1.00 / numQuantileBuckets).toArray
      val quanTileSQL = s"Array(${quantilesTarget.mkString(",")})"

      val viewName = s"`temp_${colName.trim}_vw_$dsIndex`"
      valuedf.createOrReplaceTempView(viewName)

      val sql = s"select percentile(`$colName`, $quanTileSQL) as percent from $viewName "

      //single column
      val percentDF: DataFrame=valuedf.sqlContext.sql(sql)
      val quantiles : mutable.WrappedArray[Double] = percentDF.collect().head.getAs[mutable.WrappedArray[Double]]("percent")

      val quantilesSampleCount = (1.0d* valuedf.count()) / numQuantileBuckets
      (quantiles zip quantiles.tail).map { a =>
        val (low, high) = a
        FSHistogram.Bucket(low, high, sampleCount = quantilesSampleCount)
      }

    }

  //Try to conform the data type to Googe Protobuf enum type
  private[features] def convertDataType(dfType: String) :  ProtoDataType= {

    dfType.toLowerCase match {
          case x if x == "float" || x == "double" => FLOAT
          case x if x == "long" || x == "integer" || x == "short" || x == "boolean" || x == "java.math.bigdecimal" => INT
          case x if x == "date" || x == "java.util.date" || x == "java.sql.date" || x == "java.sql.timestamp" => INT //remember to convert date to Int
          case _ => STRING
    }
  }

  /**
    * Convert Date to String format, as the UI can only handle INT, Numeric and String
   *
    * If the data frame is already in numerical format, simply return original DataFrame
    * If the data frame type is not convertable, simply return the original DataFrame (so it would be error out later)
    * Assume the dataframe is already flattened, so there should be no Array or Structure Type.
    *
    * @param columnDF -- input data frame, only only contains one column
    * @return DataFrame
    *
    */
  private[features] def convertDateDataFrame(columnDF: DataFrame): DataFrame = {
    import org.apache.spark.sql.functions._
    val spark = columnDF.sqlContext.sparkSession
    val field = columnDF.schema.fields(0)


    field.dataType match {
      case s:NumericType => columnDF
      case s:TimestampType => columnDF.select(date_format(columnDF(field.name), dateFormat)).toDF(field.name)
      case s:DateType =>      columnDF.select(date_format(columnDF(field.name), dateFormat)).toDF(field.name)
      case _ =>
        columnDF
    }

  }

  private[features] def toRowCountDF(colDF: DataFrame) : DataFrame = {

    import org.apache.spark.sql.functions._
    val spark = colDF.sqlContext.sparkSession
    import spark.implicits._
    val field = colDF.schema.fields.head

    val df = field.dataType match {
      case x: ArrayType =>
        //note: this doesn't remove NaN, empty string and null values inside the array,
        colDF.select(size(colDF(field.name)))
      case _: NullType   => colDF.map(_ => 0)
      case s:NumericType =>
        colDF.filter(!colDF(field.name).isNaN).map(_ => 1)
      case s:StringType =>
        colDF.withColumn(field.name, when(length(colDF(field.name)) > 0, 1).otherwise(lit(0)))
      case s:BooleanType =>
        colDF.withColumn(field.name, when(colDF(field.name) === true, 1).otherwise(lit(0)))
      case _ =>  colDF.map(_ => 1)
    }

    df.toDF(field.name)
  }

}
