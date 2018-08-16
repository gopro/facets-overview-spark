
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