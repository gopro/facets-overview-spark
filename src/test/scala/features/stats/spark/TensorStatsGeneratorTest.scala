package features.stats.spark

import org.tensorflow.example.{Example, Feature, Features, Int64List}
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
  */

class TensorStatsGeneratorTest extends StatsGeneratorTestBase {

  test("parse Example Int") {




   //LongWritable key, Text value,
//
//    val int64List = Int64List.newBuilder.addValue(key.get).build
//    val offset = Feature.newBuilder.setInt64List(int64List).build
//
//    val byteString = ByteString.copyFrom(value.copyBytes)
//    val bytesList = BytesList.newBuilder.addValue(byteString).build
//    val text = Feature.newBuilder.setBytesList(bytesList).build
//
//    val featu res = Features.newBuilder.putFeature("offset", offset).putFeature("text", text).build
//    val example = Example.newBuilder.setFeatures(features).build

    val examples : Seq[Example]  = (0 until 50).map { i =>
      val int64List = Int64List.newBuilder.addValue(i).build
      val num = Feature.newBuilder.setInt64List(int64List).build
      val features = Features.newBuilder.putFeature("num", num).build()
      Example.newBuilder.setFeatures(features).build
    }

//    examples.zipWithIndex.map { a =>
//      val (example, index) = a
//
//      example.getFeatures.getFeatureMap.asScala
//      val baos  = new ByteArrayOutputStream()
//      val writer = new TFRecordWriter(new DataOutputStream(baos))
//      writer.write(example.toByteArray())
//    }





  }

}
