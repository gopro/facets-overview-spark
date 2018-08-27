package facets.overview

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
//

/**
  * Instead set up the whole Spark Program (spark client and spark job).
  * Here we use Scala Test as the spark client and focused on Spark Job.
  *
  */

import java.io.File

import features.stats.spark.{DataFrameUtils, NamedDataFrame, StatsGeneratorTestBase}
import org.apache.spark.sql.DataFrame

class OverviewDemo extends StatsGeneratorTestBase {

  /**
    * This demo simulates the original facets-overview with
    * https://github.com/PAIR-code/facets/blob/master/facets_overview/Overview_demo.ipynb
    * Where the train data and test data are located at
    *
    * https://archive.ics.uci.edu/ml/machine-learning-databases/adult/adult.data
    * https://archive.ics.uci.edu/ml/machine-learning-databases/adult/adult.test
    *
    * We pre-download data set and put in the project, you can work offline
    *
    *
    *
    */

  test("generate-features-stats-demo-data") {

    //part 1: Generate DataFrames

    //original feature columns
    val columns = Array("Age", "Workclass", "fnlwgt", "Education", "Education-Num", "Marital Status",
      "Occupation", "Relationship", "Race", "Sex", "Capital Gain", "Capital Loss",
      "Hours per week", "Country", "Target")

    //normalize the feature names by removing dash, space etc.
    val features: Array[String] = DataFrameUtils.sanitizedNames(columns)

    //load train data and test data from CSV Files
    val trainData: DataFrame = loadCSVFile("src/test/resources/data/adult.data.csv")
    val testData: DataFrame = loadCSVFile("src/test/resources/data/adult.test.txt")

    //set the feature column names to the Data Frames.
    val train = trainData.toDF(features: _*)
    val test = testData.toDF(features: _*)

    //create named dataframes
    val dataframes = List(NamedDataFrame(name = "train", train), NamedDataFrame(name = "test", test))

    //part 2: generate feature statistics
    val proto = generator.protoFromDataFrames(dataframes)

    //persist protobuf binary into files (without or with base64 encoding)
    persistProto(proto, base64Encode = false, new File("src/test/resources/data/stats.pb"))
    persistProto(proto, base64Encode = true, new File("src/test/resources/data/stats.txt"))


  }


}
