package features.stats

import java.io.File

import features.stats.spark.{NamedDataFrame, StatsGeneratorTestBase}
import org.apache.spark.sql.DataFrame

class ProtoUtilsTest extends StatsGeneratorTestBase{
  import spark.implicits._

  test("proto-persist") {
    val data = Seq[String]("hi", "good", "hi", "hi", "a", "a")
    val df: DataFrame = spark.sparkContext.parallelize(data).toDF("TestFeatureString")
    val dataFrames = List(NamedDataFrame(name = "testDataSet", df))
    val p = generator.protoFromDataFrames(dataFrames, catHistgmLevel = Some(2))

    val protoFile = new File("target", "test_proto.pb")
    val path = ProtoUtils.persistProto(p, base64Encode = false, protoFile)
    assert(path.toFile.exists())
    assert(path.toFile === protoFile)
    assert(protoFile.exists())
  }

}
