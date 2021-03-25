package features.stats

import java.io.File
import java.net.URI
import java.nio.file.{Files, Paths}

import featureStatistics.feature_statistics.DatasetFeatureStatisticsList
import org.apache.spark.sql.DataFrame


object ProtoUtils {

  def persistProto(proto: DatasetFeatureStatisticsList,
                   base64Encode: Boolean = false,
                   file : File) : Unit = {
    Files.write(Paths.get(file.getPath),  protoToBytes(proto, base64Encode ))
  }

  def base64EncodeProto(proto: DatasetFeatureStatisticsList): String = {
    import java.util.Base64
    import java.nio.charset.Charset
    val  UTF8_CHARSET = Charset.forName("UTF-8")

    val b = Base64.getEncoder.encode(proto.toByteArray)
    new String(b, UTF8_CHARSET)
  }

  def protoToBytes(proto: DatasetFeatureStatisticsList, base64Encode: Boolean = false) : Array[Byte]= {
    if (base64Encode)
      base64EncodeProto(proto).getBytes()
    else
      proto.toByteArray
  }


  def loadProto(base64Encode: Boolean, file: File ): DatasetFeatureStatisticsList = {
    import java.util.Base64
    val bs = Files.readAllBytes(Paths.get(file.getPath))
    val bytes = if (base64Encode) Base64.getDecoder.decode(bs) else bs
    DatasetFeatureStatisticsList.parseFrom(bytes)
  }

  def toJson(proto: DatasetFeatureStatisticsList) : String = {
    import scalapb.json4s.JsonFormat
    JsonFormat.toJsonString(proto)
  }


}
