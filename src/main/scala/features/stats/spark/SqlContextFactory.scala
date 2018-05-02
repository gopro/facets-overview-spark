package features.stats.spark

import com.typesafe.config.Config
import config.ConfigUtil
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}

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
  */
object SqlContextFactory {

  private val INSTANTIATION_LOCK = new Object()

  /**
    * Reference to the last created HiveContext.
    */
  @transient @volatile private var lastInstantiatedContext: SQLContext = _

  /**
    * Get the singleton HiveContext if it exists or create a new one using the given SparkContext.
    * This function can be used to create a singleton HiveContext object that can be shared across
    * the JVM.
    */
  def getOrCreate(sparkContext: SparkContext, config: Option[Config] = None): SQLContext = {

    if (lastInstantiatedContext == null) {
      INSTANTIATION_LOCK.synchronized {
        if (lastInstantiatedContext == null) {
          lastInstantiatedContext = SparkSession.builder().enableHiveSupport().config(sparkContext.getConf).getOrCreate().sqlContext

          initializeContext(lastInstantiatedContext, config)
        }
      }
    }
    lastInstantiatedContext
  }

  /**
    * Initialize the context.
    *
    * @param sqlContext -- Spark SqlContext
    * @param config -- TypeSafe config
    */
  def initializeContext(sqlContext: SQLContext, config: Option[Config] = None): Unit = {
    // Enable dynamic partitioning since partitions are created dynamically in these Spark jobs.
    sqlContext.setConf("hive.exec.dynamic.partition", "true")
    sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    // The built in Parquet support in Spark SQL has some caching bugs where newly created partition files are
    // not correctly cached after insertion.  So we will disable the built in support and use the Hive SerDe
    // for the Parquet tables.
    sqlContext.setConf("spark.sql.hive.convertMetastoreParquet", "false")
    // Set this to false to avoid seeing the message
    // 'Warning: fs.defaultFs is not set when running "chmod" command.'
    // thousands of times when running locally (e.g. UT execution or mvn build).
    // Note this can not be achieved by setting it on the hadoop configuration
    // within the spark context.
    sqlContext.setConf("hadoop.shell.missing.defaultFs.warning", "false")
    config.foreach(x => {
      ConfigUtil.toMap(x).foreach(
        kv => {
          sqlContext.setConf(kv._1, kv._2)
        })
    })
  }
}
