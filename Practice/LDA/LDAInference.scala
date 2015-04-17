/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io._
import org.apache.spark.Logging
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

import LDAUtil._

object LDAInference extends App with Logging {
    override def main(args: Array[String]) {
        val appStartedTime = System.currentTimeMillis()
        val numTopics = args(0).toInt
        val totalIter = args(1).toInt
        val burnInIter = args(2).toInt

        println("$$$$$$$$$$$$numTopics = %d, totalIter = %d, burnInIter = %d"
                .format(numTopics, totalIter, burnInIter))

        val inputRootPath = args(3)
        val docsPath = inputRootPath + args(4)
        val modelPath = args(5) //"/home/spark/workspace/quinzh/data/testOutput/news/title/LDAModel_0.01_0.01_1.00_34.35_10_2"
        println("$$$$$$$$$$$$docsPath = %s".format(docsPath))
        println("$$$$$$$$$$$$modelPath = %s".format(modelPath))

        val outputRootPath = args(6) + "inference/" //"/home/spark/workspace/quinzh/data/testOutput/newsOneWeek_Frq_10/body/"
        val termNumPath = inputRootPath + "CorpusVocData.size"
        val vocsPath = inputRootPath + "Corpus.voc"

        val conf = new SparkConf().setAppName("LDAInference")
        val sc = new SparkContext(conf)

        val numTerms = sc.textFile(termNumPath).map(numStr => numStr.toInt).reduce(_ + _)
        val vocs = sc.textFile(vocsPath).map(line => line.split('\t')).map(pair => pair(1)).collect()

        val model = restoreModel(sc, modelPath, vocs, numTopics, numTerms)

        val testDocs = readDocsFromTxt(sc, docsPath, numTerms)
        val inferenceStartedTime = System.currentTimeMillis()
        val testDocTopics = testDocs.map(doc => (doc._1, model.inference(doc._2, totalIter, burnInIter)))

        val inferenceEndedTime = System.currentTimeMillis()
        val outputPath = generateOutputPath(model, outputRootPath, totalIter, burnInIter)

        outputTestResults(sc, testDocTopics, outputPath)

        val appEndedTime = System.currentTimeMillis()
        println("-----------------------------Inference time consumed: %f s".format(
                     (inferenceEndedTime - inferenceStartedTime) / 1e3))
        println("-----------------------------Total time consumed: %f s".format((
                     appEndedTime - appStartedTime) / 1e3))
    }
}
