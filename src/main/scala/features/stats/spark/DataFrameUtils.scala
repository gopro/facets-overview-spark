package features.stats.spark

import org.apache.spark.sql.types.{ArrayType, StructField}
import org.apache.spark.sql.{Column, DataFrame}

/**
  * Created by chesterchen on 6/2/18.
  */
object DataFrameUtils {

  def isNestedArrayType(field : StructField): Boolean = {
    field.dataType match {
      // example: tf.train.SequenceExample
      case ArrayType(ArrayType(_, _), _) => true
      case _ => false
    }
  }


  /**
    * cartentian product type of flatten. All array data are exploded against all other columns
    *
    * @param dataFrame -- input dataframe
    * @param recursive -- true for recursive flatten, that is array of array will be exploded as well.
    * @return
    */
  private[spark] def flatten(dataFrame: DataFrame, recursive: Boolean = true) : DataFrame = {
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
          val cols : Seq[Column]=
            if (index == 0)
              explode(df(name)).as(name)::fieldNames.tail.map(_._1).map(df(_)).toList
            else if (index == size-1) {
              fieldNames.take(index).map(_._1).map(df(_)).toList ++ List( explode(df(name)).as(name))
            }
            else {
              val restLen = size - (index+1)
              val f1stPart =fieldNames.take(index).map(_._1).map(df(_)).toList
              val f2ndPart=fieldNames.takeRight(restLen).map(_._1).map(df(_)).toList
              f1stPart ++ (explode(df(name)).as(name)::f2ndPart)
            }

          df = df.select(cols:_*)
          if (recursive) flatten(df) else df
        case _ => df
      }
      df
    }

    df
  }


}
