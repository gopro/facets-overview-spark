package features.stats.spark


import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.tagobjects.Slow
import org.scalatest.{BeforeAndAfterAll, FunSuite}

/**
  * Created by chesterchen on 5/10/18.
  */
class TensorConnectorTest extends FunSuite with BeforeAndAfterAll{

  val appName = "TensorConnector"
  val SPARK_MASTER_URL = "local[2]"
  var sc: SparkContext = _
  var sqlContext: SQLContext = _
  var spark: SparkSession = _

  override protected def beforeAll(): Unit = {
    val sparkConf = new SparkConf().setMaster(SPARK_MASTER_URL).setAppName(appName)
    sc = SparkContext.getOrCreate(sparkConf)
    sqlContext = SqlContextFactory.getOrCreate(sc)
    spark = sqlContext.sparkSession
  }



  val exampleSchema = StructType(List(
    StructField("id", IntegerType),
    StructField("IntegerLabel", IntegerType),
    StructField("LongLabel", LongType),
    StructField("FloatLabel", FloatType),
    StructField("DoubleLabel", DoubleType),
    StructField("DoubleArrayLabel", ArrayType(DoubleType, true)),
    StructField("StrLabel", StringType),
    StructField("BinaryLabel", BinaryType)))

  val exampleTestRows: Array[Row] = Array(
    new GenericRow(Array[Any](11, 1, 23L, 10.0F, 14.0, List(1.0, 2.0), "r1",
      Array[Byte](0xff.toByte, 0xf0.toByte))),
    new GenericRow(Array[Any](21, 2, 24L, 12.0F, 15.0, List(2.0, 2.0), "r2",
      Array[Byte](0xff.toByte, 0xf1.toByte))))


  val sequenceExampleTestRows: Array[Row] = Array(
    new GenericRow(Array[Any](23L, Seq(Seq(2.0F, 4.5F)), Seq(Seq("r1", "r2")))),
    new GenericRow(Array[Any](24L, Seq(Seq(-1.0F, 0F)), Seq(Seq("r3")))))

  val sequenceExampleSchema = StructType(List(
    StructField("id",LongType),
    StructField("FloatArrayOfArrayLabel", ArrayType(ArrayType(FloatType))),
    StructField("StrArrayOfArrayLabel", ArrayType(ArrayType(StringType)))
  ))

  private def createDataFrameForExampleTFRecord() : DataFrame = {
    val rdd = spark.sparkContext.parallelize(exampleTestRows)
    spark.createDataFrame(rdd, exampleSchema)
  }

  private def createDataFrameForSequenceExampleTFRecords() : DataFrame = {
    val rdd = spark.sparkContext.parallelize(sequenceExampleTestRows)
    spark.createDataFrame(rdd, sequenceExampleSchema)
  }


  private def getRowValue(r: Row, f: StructField) : Any = {

    f.dataType match {
      case IntegerType => r.getAs[Int](f.name)
      case StringType => r.getAs[String](f.name)
      case LongType => r.getAs[Long](f.name)
      case ArrayType(DoubleType, true) => r.getAs[Array[Double]](f.name)
      case _ => r.getAs[String](f.name)
    }
  }

  test("load tfrecords") {

    val path = "target/test-output.tfrecord"
    val testRows: Array[Row] = Array(
      new GenericRow(Array[Any](11, 1, 23L, 10.0F, 14.0, List(1.0, 2.0), "r1")),
      new GenericRow(Array[Any](21, 2, 24L, 12.0F, 15.0, List(2.0, 2.0), "r2")))

    val schema = StructType(List(StructField("id", IntegerType),
      StructField("IntegerTypeLabel", IntegerType),
      StructField("LongTypeLabel", LongType),
      StructField("FloatTypeLabel", FloatType),
      StructField("DoubleTypeLabel", DoubleType),
      StructField("VectorLabel", ArrayType(DoubleType, true)),
      StructField("name", StringType)))

    val rdd = spark.sparkContext.parallelize(testRows)

    //Save DataFrame as TFRecords
    val df: DataFrame = spark.createDataFrame(rdd, schema)
    df.write.mode(SaveMode.Overwrite).format("tfrecords")
      .option("recordType", "Example")
      .save(path)

    //Read TFRecords into DataFrame.
    //The DataFrame schema is inferred from the TFRecords if no custom schema is provided.
    val importedDf1: DataFrame = spark.read.format("tfrecords").option("recordType", "Example").load(path)
    importedDf1.printSchema()

    //assert field name
    df.schema.fieldNames.foreach{ f =>
      val imported = importedDf1.schema.fieldNames.find(f2 => f2 == f)
      assert(imported.isDefined, f + " not found")
      assert(imported.get === f)
    }

    //data type matches
    df.schema.foreach{ f =>
      val imported = importedDf1.schema.find(f2 => f2.name == f.name && f2.dataType == f.dataType)
      if (imported.isEmpty) {
        println(s"Field '${f.name}' with '${f.dataType}' is not found," +
        s"'${f.name}' with '${importedDf1.schema.find(f2 => f2.name == f.name).get.dataType}' found instead.")
      }
    }


    //Read TFRecords into DataFrame using custom schema
    val importedDf2: DataFrame = spark.read.format("tfrecords").schema(schema).load(path)
    df.schema.foreach{ f =>
      val imported = importedDf2.schema.find(f2 => f2 == f)
      assert(imported.isDefined)
      assert(imported.get === f)
      val xs = df.select(f.name).collect().map(r => getRowValue(r, f))
      val ys = importedDf2.select(f.name).collect().map(r => getRowValue(r, f))
      assert(xs === ys)
    }


  }

  test ("load tfrecords2") { //simpler way to do this.

    val path = "target/test-output.tfrecord2"
    val spark = sqlContext.sparkSession
    import spark.implicits._

    val testRows = Array((11, 1, 23L, 10.0F, 14.0D, List(1.0D, 2.0D), "r1"), (21, 2, 24L, 12.0F, 15.0D, List(2.0D, 2.0D), "r2") )
    val features = Array("id", "IntegerTypeLabel", "LongTypeLabel","FloatTypeLabel","DoubleTypeLabel", "VectorLabel", "name")
    val rdd = spark.sparkContext.parallelize(testRows)
    //Save DataFrame as TFRecords
    val df = rdd.toDF(features: _*)
    df.printSchema()
    df.write.mode(SaveMode.Overwrite).format("tfrecords").option("recordType", "Example").save(path)

    //Read TFRecords into DataFrame using custom schema
    val importedDf2: DataFrame = spark.read.format("tfrecords").schema(df.schema).load(path)

    df.schema.foreach{ f =>
      val imported = importedDf2.schema.find(f2 => f2 == f)
      assert(imported.isDefined)
      assert(imported.get === f)
      val xs = df.select(f.name).collect().map(r => getRowValue(r, f))
      val ys = importedDf2.select(f.name).collect().map(r => getRowValue(r, f))
      assert(xs === ys)

    }

  }


  test("Test Import/Export of SequenceExample records") {

    val path = "target/sequenceExample.tfrecord"

    val df: DataFrame = createDataFrameForSequenceExampleTFRecords()
    df.write.mode(SaveMode.Overwrite).format("tfrecords").option("recordType", "SequenceExample").save(path)

    val importedDf: DataFrame = spark.read.format("tfrecords").option("recordType", "SequenceExample").schema(sequenceExampleSchema).load(path)
    val actualDf = importedDf.select("id", "FloatArrayOfArrayLabel", "StrArrayOfArrayLabel").sort("id")

    val expectedRows = df.collect()
    val actualRows = actualDf.collect()

    assert(expectedRows === actualRows)
  }


  ignore("load yourtube video",Slow){

    //Import Video-level Example dataset into DataFrame
    val videoSchema = StructType(List(StructField("video_id", StringType),
      StructField("labels", ArrayType(IntegerType, true)),
      StructField("mean_rgb", ArrayType(FloatType, true)),
      StructField("mean_audio", ArrayType(FloatType, true))))



        val videoDf: DataFrame = spark.read.format("tfrecords")
                                      .schema(videoSchema)
                                      .option("recordType", "Example")
                                      .load("file:///tmp/video_level-train-0.tfrecord")
        videoDf.show()
        videoDf.write.format("tfrecords")
               .option("recordType", "Example")
               .save("target/youtube-8m-video.tfrecord")

        val importedDf1: DataFrame = spark.read.format("tfrecords")
                                          .option("recordType", "Example")
                                          .schema(videoSchema)
                                          .load("target/youtube-8m-video.tfrecord")
        importedDf1.show()

        //Import Frame-level SequenceExample dataset into DataFrame
        val frameSchema = StructType(List(StructField("video_id", StringType),
          StructField("labels", ArrayType(IntegerType, true)),
          StructField("rgb", ArrayType(ArrayType(StringType, true),true)),
          StructField("audio", ArrayType(ArrayType(StringType, true),true))))
        val frameDf: DataFrame = spark.read.format("tfrecords")
                                      .schema(frameSchema)
                                      .option("recordType", "SequenceExample")
                                      .load("file:///tmp/frame_level-train-0.tfrecord")

        frameDf.show()
        frameDf.write.format("tfrecords").option("recordType", "SequenceExample").save("youtube-8m-frame.tfrecord")
        val importedDf2: DataFrame = spark.read.format("tfrecords").option("recordType", "SequenceExample").schema(frameSchema).load("youtube-8m-frame.tfrecord")
        importedDf2.show()
  }



}
