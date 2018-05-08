  package features.stats.spark
import featureStatistics.feature_statistics.FeatureNameStatistics.Type._
import featureStatistics.feature_statistics.FeatureNameStatistics.{Type => ProtoDataType}
import featureStatistics.feature_statistics.StringStatistics.FreqAndValue
import featureStatistics.feature_statistics.{Histogram => FSHistogram, _}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Row}

import scala.collection.mutable

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
 **
  *
   * Class for generating the feature_statistics protobuf.
   * The protobuf is used as input for the Overview visualization.
   * This class follows the GOOGLE FACETS facet_overview python implementation logic
   * The Spark DataFrame is used instead of using Python numpy and pandas
  *
  * NOTE: Now this doesn't handle tf.sampels, tf.sequenceSamples.
  *
  *
  * Each dataset has DatasetFeatureStatistics which contains lists metadata (name of dataset, number of sample, and Seq[FeatureNameStatistics])
  * Each FeatureNameStatistics is feature stats for the given dataset, which includes feature name and data type and one of (num_stats, string_stats or binary_stats)
  * NumericStatistics
  *     CommonStatistics
  *     (std_dev,mean, median,min, max, histogram)
  *     WeightedNumericStatistics weighted_numeric_stats
  * StringStatistics
  *  CommonStatistics common_stats = 1;
       (unique values, FreqAndValue {value, frequency}, FreqAndValue top_values = 3,avg_length = 4
       rank_histogram )
       weighted_string_stats;
  *
  */


case class NamedDataFrame(name:String, data: DataFrame)
case class DataEntrySet(name: String, size: Long, entries : Array[DataEntry])

/**
  *
  * @param featureName
  * @param `type`
  * @param values
  * @param counts
  * @param missing
  * @param feat_lens -- Reserved for Tensorflow, not sure this should be DataFrame or Array,
  *                     depending how do we integrate with tensorflow records
  */
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

object FeatureStatsGenerator {

  case class NonZeroInfinite(isNan: Int, isZero: Int, isPosInfinite: Int, isNegInfinite: Int)

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
  private[features]  def convertDate2Long(value : Any) : Long = {
    value match {
      case t:java.util.Date => t.getTime
      case _ => if  (value == null) 0 else {
        val x = value.toString.trim
        if (x.isEmpty) 0 else x.toLong
      }
    }
  }
  private[features]  def convertDateToLong: UserDefinedFunction = udf((value:Any) => convertDate2Long(value))

  def  nonZeroOrEmpty: UserDefinedFunction= udf((value : Any) =>nonZeroOrEmptyValue(value))
  val CheckZeroInfinite : UserDefinedFunction= udf((value : Double) =>checkZeroNanInfiniteFun(value))

}
class FeatureStatsGenerator(datasetProto: DatasetFeatureStatisticsList) {

  import FeatureStatsGenerator._

  /**
    * Creates a feature statistics proto from a set of pandas data frames.
    *
    * @param dataFrames         A list of dicts describing tables for each dataset for the proto.
    *                           Each entry contains a 'table' field of the dataframe of the data and a 'name' field
    *                           to identify the dataset in the proto.
    * @param histgmCatLevelsCount int, controls the maximum number of levels to display in histograms
    *                           for categorical features. Useful to prevent codes/IDs features
    *                           from bloating the stats object. Defaults to None.
    * @return The feature statistics proto for the provided tables.
    */
  def protoFromDataFrames(dataFrames: List[NamedDataFrame],
                          features : Set[String] = Set.empty[String],
                          histgmCatLevelsCount:Option[Int]=None): DatasetFeatureStatisticsList = {

    val datasets: List[DataEntrySet] = toDataEntries( dataFrames)
    genDatasetFeatureStats(datasets, features, histgmCatLevelsCount)

  }

  /**
    *
    * @param colDF: single column DataFrame
    * @return exploded single columnDataFrame
    */
  private def flattenOneColumn(colDF:DataFrame) : DataFrame = {

    val spark = colDF.sqlContext.sparkSession
    import org.apache.spark.sql.functions._
    val field = colDF.schema.fields(0)

    val df = field.dataType match {
      case x: ArrayType =>
        flattenOneColumn(colDF.select(explode(colDF(field.name)))
                             .as(field.name + "_flatten"))
      case _ => colDF
    }

    df
  }

  private def flatten(dataFrame: DataFrame) : DataFrame = {
    val spark = dataFrame.sqlContext.sparkSession
    import org.apache.spark.sql.functions._
    var df = dataFrame
    val size = df.schema.fields.length
    (0 until size).foreach {index =>

      var fieldNames :Seq[(String,Int)]= df.schema.fieldNames.zipWithIndex
      val name = fieldNames.find(a => a._2 == index).get._1
      val dt = df.schema.fields.find(f => f.name == name).head.dataType

      df = dt match {
        case x: ArrayType =>
          val cols : Seq[Column]=  if (index == 0)
              explode(df(name)).as(s"${name}_flatten")::fieldNames.tail.map(_._1).map(df(_)).toList
          else if (index == size-1) {
            fieldNames.take(index).map(_._1).map(df(_)).toList ++ List( explode(df(name)).as(s"${name}_flatten"))
          }
          else {
            val restLen = size - (index+1)
            val f1stPart =fieldNames.take(index).map(_._1).map(df(_)).toList
            val f2ndPart=fieldNames.takeRight(restLen).map(_._1).map(df(_)).toList
            f1stPart ++ (explode(df(name)).as(s"${name}_flatten")::f2ndPart)
          }

          df = df.select(cols:_*)
          flatten(df)
        case _ => df
      }
      df
    }

    df
 }



  private[spark] def toRowCountDF(colDF: DataFrame) : DataFrame = {

    import org.apache.spark.sql.functions._
    val spark = colDF.sqlContext.sparkSession
    import spark.implicits._
    val field = colDF.schema.fields(0)

    val df = field.dataType match {
      case x: ArrayType =>
        //fixme: this doesn't remove NaN, empty string and null values inside the array,
        //      count is more than it should be
        colDF.select(size(colDF(field.name)))

      case x: BinaryType => colDF.map(_ => 1)
      case x: StructType => colDF.map(_ => 1)
      case x: MapType    => colDF.map(_ => 1) //not sure this is correct.
      case _: NullType   => colDF.map(_ => 0)
      case s:NumericType =>
        val newDF = colDF.filter(!colDF(field.name).isNaN)
        newDF.map(_ => 1)
      case s:StringType =>
        colDF.withColumn(field.name, when(length(colDF(field.name)) > 0, 1).otherwise(lit(0)))
      case s:BooleanType =>
        colDF.withColumn(field.name, when(colDF(field.name) === true, 1).otherwise(lit(0)))
      //this shouldn't happen, the timestamp should already converted to numerical data
      case s:TimestampType => colDF.map(_ => 1)

      //this shouldn't happen, the timestamp should already converted to numerical data
      case s:CalendarIntervalType => colDF.map(_ => 1)
      case _ =>  colDF.map(_ => 1)

    }

    df.toDF(field.name)
  }

  private[features] def toDataEntries(dataFrames: List[NamedDataFrame]) : List[DataEntrySet] = {

    println("convert to DataEntries")

    val datasets: List[DataEntrySet] = dataFrames.map { ndf =>
      val df = ndf.data
      val flattenDF = flatten(df)
      val origSize = flattenDF.count()

      val dataEntries = flattenDF.schema.fields map { f =>
        val colName = f.name
        val protoTypeName = convertDataType(f.dataType.typeName)
        val convertedDF = convertToNumberDataFrame(df.select(f.name))
        //Remove all null and nan values and count how many were removed.
        //also remove "NaN" strings
        val filteredFlattenedDF = convertedDF.na.drop().filter(!convertedDF(colName).isNaN)
        val missingCount = origSize - filteredFlattenedDF.count()

        val featureDF = df.select(f.name)
        //get RowCount DataFrame
        val rowCountDF = toRowCountDF(featureDF)
        DataEntry(featureName=f.name,//todo:fixme,flatten column name maybe different from feature name
                  `type` = protoTypeName,
                  values = filteredFlattenedDF,
                  counts = rowCountDF,
                  missing = missingCount)
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
    * @param histgmCatLevelsCount controls the maximum number of
    *                             levels to display in histograms for categorical features.
    *                             Useful to prevent codes/IDs features from bloating the stats object.
    *                             Defaults to None.
    * @return
    *         The feature statistics proto for the provided datasets.
    */

  private[features] def genDatasetFeatureStats(datasets : List[DataEntrySet],
                                               features : Set[String] = Set.empty[String],
                                               histgmCatLevelsCount: Option[Int] = None):DatasetFeatureStatisticsList = {

    println("DataEntries to protobuf")
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
            .withStringStats(getStringStats( entry, ds.size, dsIndex, histgmCatLevelsCount))
        }
      }

      DatasetFeatureStatistics(ds.name, numExamples = ds.size, features = feat)
    }

    val xs = dfsList.foldLeft(allDatasets) { (z,dfs) =>
       z.addDatasets(dfs)
    }

    xs
  }

  private[features] def getCommonStats(dsSize: Long, entry: DataEntry, numNan: Long, dsIndex:Int) : CommonStatistics = {

    val countDF= entry.counts

    val countQuantileBuckets = populateQuantilesHistogramBucket(entry.counts,dsIndex)
    val countQuantileHist = FSHistogram(numNan = numNan,
                                        buckets = countQuantileBuckets,
                                        `type` = FSHistogram.HistogramType.QUANTILES)


    val featureListLenHist :Option[FSHistogram]
            = entry.feat_lens.map { featLens =>
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

  private[features] def getBasicNumStats(valueDF: DataFrame, colName: String, dsIndex:Int) : BasicNumStats = {
    println(s"getBasicNumStats for ${ colName}")

    val spark = valueDF.sqlContext.sparkSession
    import spark.implicits._

    val filteredValueDF = valueDF.filter(r => isFiniteFun(r.getAs(0)))
    valueDF.createOrReplaceTempView(s"`${colName}_view_$dsIndex`")

    val sumDF = valueDF.mapPartitions { it =>
      it.map { row =>
        val a : NonZeroInfinite = checkZeroNanInfiniteFun(row.getAs[Double](0))
        (colName, a.isZero, a.isPosInfinite, a.isNegInfinite, a.isNan, 1)
      }
    }.toDF(colName, "isZero", "isPosInfi", "isNegInfi", "isNan", "hasValue")
      .groupBy(colName)
      .sum()
      .toDF(colName, "zeroCount", "posInfiCount", "negaInfiCount", "nanCount", "valueCount")

    val countsDF = sumDF.select($"zeroCount", $"posInfiCount", $"negaInfiCount", $"nanCount", $"valueCount")
    val indexedFields = countsDF.schema.fields.zipWithIndex
    val extraCounts = countsDF.collect.flatMap(row => indexedFields.map(f => f._1.name -> row.getAs[Long](f._2))).toMap

    val valueCount : Long = extraCounts("valueCount")
    val numNan     : Long = extraCounts("nanCount")
    val numZeros   : Long = extraCounts("zeroCount")
    val numPosinf  : Long = extraCounts("posInfiCount")
    val numNeginf  : Long = extraCounts("negaInfiCount")

    //          # Remove all non-finite (including NaN) value from the numeric
    //          # valueDF in order to calculate histogram buckets/counts. The
    //          # inf valueDF will be added back to the first and last buckets.

    val rdd : RDD[Double]= filteredValueDF.rdd.map{ a =>
      a.get(0) match {
        case x:Double => x
        case x:Float => x.toDouble
        case x:Long => x.toDouble
        case x:Int => x.toDouble
        case x:Short => x.toDouble
      }
    }

    val stddevVal = rdd.stdev()
    val meanVal = rdd.mean()
    val maxVal = rdd.max()
    val minVal = rdd.min()
    val medianVal = filteredValueDF.stat.approxQuantile(colName, Array(0.5), 10e-5).head


    val bucketCount = 10
    val stats = BasicNumStats(colName, numCount = valueCount, numNan, numZeros, numNeginf,numPosinf,
                  stddev = stddevVal,
                  mean = meanVal,
                  min=  minVal,
                  median=medianVal,
                  max = maxVal,
                  histogram =rdd.histogram(bucketCount)
                )

    stats

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
                                                 basicNumStats: BasicNumStats): Seq[FSHistogram.Bucket] = {

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
        if (basicNumStats.numNeginf > 0)
          Seq((Double.NegativeInfinity, Double.NegativeInfinity, basicNumStats.numNeginf))
        else if (basicNumStats.numPosinf > 0)
          Seq((Double.PositiveInfinity, Double.PositiveInfinity, basicNumStats.numPosinf))
        else
          Seq.empty
      }

    ys.map(a => Bucket(lowValue = a._1, highValue = a._2, sampleCount = a._3))
  }


  private[features] def getNumericStats(entry: DataEntry, dsSize:Long, dsIndex :Int) : NumericStatistics = {

    println(s"get numeric stats for ${ entry.featureName}")

    val valueDF: DataFrame   = entry.values
    val featureName:String   = entry.featureName
    val countDF : DataFrame  = entry.counts

    val basicStats  = getBasicNumStats(valueDF, featureName, dsIndex)
    val stdBuckets  = updateStdHistogramBucket(featureName, basicStats)
    val stdHist     = FSHistogram(numNan = basicStats.numNan,
                                  buckets = stdBuckets,
                                  `type` = FSHistogram.HistogramType.STANDARD)

    val filteredValueDF = valueDF.filter(r => isFiniteFun(r.getAs(0)))
    val qBuckets        = populateQuantilesHistogramBucket(filteredValueDF,dsIndex)
    val qHist           = FSHistogram(numNan = basicStats.numNan,
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

    println(s"getStringStats for ${ entry.featureName}")

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
                    RankHistogram.Bucket(lowRank = index, highRank = index, sampleCount = count, label = cat)
                  }

    val top2Buckets = buckets.take(2).map {b =>FreqAndValue(value = b.label, frequency = b.sampleCount)}

    StringStatistics(Some(commonStats),
                     topValues      = top2Buckets,
                     unique         = distinctCount,
                     avgLength      = avgLen.toFloat,
                     rankHistogram  = Some(RankHistogram(buckets)))
  }


  //Quantile histogram (different from data histogram)
  def populateQuantilesHistogramBucket(valuedf: DataFrame, dsIndex:Int) : Seq[FSHistogram.Bucket]= {

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

  //Try to confirm the data type to Googe Protobuf enum type
  private[features] def convertDataType(dfType: String) :  ProtoDataType= {

    dfType.toLowerCase match {
          case x if x == "float" || x == "double" => FLOAT
          case x if x == "long" || x == "integer" || x == "short" || x == "boolean" || x == "java.math.bigdecimal" => INT
          case x if x == "date" || x == "java.util.date" || x == "java.sql.date" || x == "java.sql.timestamp" => INT //remember to convert date to Int
          case _ => STRING
    }
  }

  val isFinite: UserDefinedFunction= udf((value : Double) =>isFiniteFun(value))


  /**
    * Convert to one column dataFrame to numerical data frame for statistical analysis
    * If the data frame is already in numerical format, simply return original DataFrame
    * If the data frame type is not convertable, simply return the original DataFrame (so it would be error out later)
    *
    * Assume the dataframe is already flattened, so there should be no Array or Structure Type.
    *
    * @param columnDF -- input data frame, only only contains one column
    * @return DataFrame
    *
    */
  private def convertToNumberDataFrame(columnDF: DataFrame): DataFrame = {
    val spark = columnDF.sqlContext.sparkSession
    val field = columnDF.schema.fields(0)

    val df = field.dataType match {
      case s:NumericType => columnDF
      case s:TimestampType => columnDF.select(convertDateToLong(columnDF(field.name))).toDF(field.name)
      case s:DateType =>
        columnDF.select(convertDateToLong(columnDF(field.name))).toDF(field.name)
      //case s:CalendarIntervalType => columnDF //not sure how to convert
      case _ =>
        columnDF
    }

    //df.show(2)
    df

  }

}
