package features.stats.spark

import featureStatistics.feature_statistics.FeatureNameStatistics.{Type => ProtoDataType}
import org.apache.spark.sql.DataFrame



/**
  * # ==============================================================================
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
  **/

/**
  * Data Structures needed to generate Feature Stats
  *
  */



case class NamedDataFrame(name:String, data: DataFrame)

case class DataEntrySet(name     : String,
                        size     : Long,
                        entries  : Array[DataEntry])

case class DataEntry(featureName : String,
                     `type`      : ProtoDataType,
                     values      : DataFrame,
                     counts      : DataFrame,
                     missing     : Long,
                     featLens    : Option[DataFrame] = None)

case class BasicNumStats(name: String,
                         numCount  : Long = 0L,
                         numNan    : Long = 0L,
                         numZeros  : Long = 0L,
                         numPosinf : Long = 0,
                         numNeginf : Long = 0,
                         stddev    : Double = 0.0,
                         mean      : Double = 0.0,
                         min       : Double = 0.0,
                         median    : Double = 0.0,
                         max       : Double = 0.0,
                         histogram : (Array[Double], Array[Long]) )

case class BasicStringStats(name     : String,
                            numCount : Long = 0,
                            numNan   : Long = 0L)





object FeatureStats
