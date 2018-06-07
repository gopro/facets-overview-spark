package features.stats.spark

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, functions}

import scala.collection.mutable.{Set => MSet}

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
    * Flatten the names of all attributes of nested structures into one "flat" structure where the name of the
    * attribute is the concatenation of all names of the parent structures down to the actual name of the attribute
    * being flattened with periods being inserted between the names.
    *
    * @param schema a StructType describing the schema or structure of a data frame
    * @param prefix a prefix to prepend to all flattened column names
    *
    * @return an array of columns with flattened names
    */
  private[spark] def flattenSchemaNames(schema: StructType, prefix: String = null): Array[Column] = {
    schema.fields.flatMap(f => {
      val colName = if (prefix == null) f.name else s"$prefix.${f.name}"

      f.dataType match {
        case st: StructType => flattenSchemaNames(st, colName)
        case _ => Array(col(colName))
      }
    })
  }


  /**
    * Flatten the nested Spark data frame into one "flat" structure.
    * Also, explode the arrayType columns into multiple rows.
    *
    * @return
    */
  def flattenDataFrame(df: DataFrame, recursive: Boolean = true): DataFrame = {
    var flattenedDf = df.select(flattenSchemaNames(df.schema)
                        .map(col => col.alias(sanitizedName(col))): _*)

    flattenedDf.schema.fields.foreach(x => {
      x.dataType match {
        case st: ArrayType =>
            // only explode the array when it is not empty otherwise it will lose the entry
            val explodedCol = functions.explode(when(size(col(x.name)) > 0, col(x.name))
                                                 .otherwise(array(lit(null).cast(st.elementType))))

            val df = flattenedDf.withColumn(x.name, explodedCol)
            flattenedDf = if(recursive) flattenDataFrame(df, recursive) else df

        case _ => //ignore
      }
    })
    flattenedDf
  }

  private def sanitizedName(col: Column) = {
    col.toString().replace(".", "_").replace("-", "_").replace("__", "_").replaceFirst("^_", "").toLowerCase
  }
}
