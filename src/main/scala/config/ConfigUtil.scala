package config
import com.typesafe.config.Config

/**
  * Created by chesterchen on 3/25/18.
  */
object ConfigUtil {

  def toMap(kvConfig: Config): Map[String, String] = {
    import scala.collection.JavaConverters._
    kvConfig.entrySet().asScala.map { entry =>
      entry.getKey -> entry.getValue.unwrapped().toString
    }(collection.breakOut)
  }
}