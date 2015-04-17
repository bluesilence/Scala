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
import java.util.Random
import breeze.linalg.{DenseVector => BDV, SparseVector => BSV}
import org.apache.spark.Logging
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.linalg.{Vectors, DenseVector => SDV, SparseVector => SSV}
import org.apache.spark.rdd.RDD

import LDAUtil._

object LDATrain extends App with Logging {
    def runTraining(
            sc: SparkContext,
            outputRootPath: String,
            vocabulary: Array[String],
            numTopics: Int,
            totalIter: Int,
            burnInIter: Int,
            alpha: Double,
            beta: Double,
            alphaAS: Double,
            trainingDocs: RDD[(Long, SSV)]): (LDAModel, Double) = {
        val trainingStartedTime = System.currentTimeMillis()
        val model = LDA.train(trainingDocs, numTopics, totalIter, burnInIter, alpha, beta, alphaAS)
        val trainingEndedTime = System.currentTimeMillis()

        val outputPath = generateOutputPath(model, outputRootPath, totalIter, burnInIter)
        outputModel(sc, model, vocabulary, outputPath)

        (model, (trainingEndedTime - trainingStartedTime) / 1e3);
    }

    def runInference(
            sc: SparkContext,
            outputRootPath: String,
            model: LDAModel,
            vocabulary: Array[String],
            numTopics: Int,
            totalIter: Int,
            burnInIter: Int,
            alpha: Double,
            beta: Double,
            alphaAS: Double,
            testDocs: RDD[(Long, SSV)]): Double = {
        val inferenceStartTime = System.currentTimeMillis()
        val testDocTopics = testDocs.map(doc => (doc._1, model.inference(doc._2, totalIter, burnInIter)))
        val inferenceEndTime = System.currentTimeMillis()

        val outputPath = generateOutputPath(model, outputRootPath, totalIter, burnInIter)
        outputTestResults(sc, testDocTopics, outputPath)

        (inferenceEndTime - inferenceStartTime) / 1e3;
    }

    override def main(args: Array[String]) {
        val numTopics = args(0).toInt
        val alpha = args(1).toDouble
        val beta = args(2).toDouble
        val alphaAS = args(3).toDouble
        val totalIter = args(4).toInt
        val burnInIter = args(5).toInt
        val numHeldOutDocs = args(6).toInt

        assert(numTopics > 0)
        assert(alpha > 0)
        assert(beta > 0)
        assert(alphaAS > 0)
        assert(totalIter > 0)
        assert(burnInIter > 0)
        assert(numHeldOutDocs >= 0)
        
        val appStartedTime = System.currentTimeMillis()
        val hdfsPath = args(7) //"hdfs://10.190.172.89:8020"
        val rootPath = hdfsPath + args(8) //"/user/spark/quinzh/"
        val checkpointPath = rootPath + "checkpoint/"
        val inputRootPath = rootPath + args(9) //"testInput/newsOneWeek_Frq_10/body/"
        val outputRootPath = args(10) //"/home/spark/workspace/quinzh/data/testOutput/newsOneWeek_Frq_10/body/"
        val termNumPath = inputRootPath + "CorpusVocData.size"
        val docsPath = inputRootPath + "CorpusTrain.libsvm"
        val vocsPath = inputRootPath + "Corpus.voc"
        val conf = new SparkConf().setAppName("LDATrain")
        val sc = new SparkContext(conf)
        sc.setCheckpointDir(checkpointPath)

        println("$$$$$$$$$$$$numTopics = %d, totalIter = %d, burnInIter = %d"
                .format(numTopics, totalIter, burnInIter))
        println("$$$$$$$$$$$$alpha = %f, beta = %f, alphaAS = %f"
                .format(alpha, beta, alphaAS))
        println("$$$$$$$$$$$$docsPath = %s".format(docsPath))

        val numTerms = sc.textFile(termNumPath).map(numStr => numStr.toInt).reduce(_ + _)
        val vocs = sc.textFile(vocsPath).map(line => line.split('\t')).map(pair => pair(1)).collect()
        var trainingTime = 0.0
        var inferenceTime = 0.0

        if (numHeldOutDocs > 0) { // Sample test docs out of all the docs
            val docsPair = readTrainingAndHeldOutDocsFromTxt(sc, docsPath, numTerms, numHeldOutDocs)
            val trainingDocs = docsPair._1
            val testDocs = docsPair._2
            val modelAndTrainingTime = runTraining(sc, outputRootPath, vocs, numTopics, totalIter, burnInIter, alpha, beta, alphaAS, trainingDocs)
            trainingTime = modelAndTrainingTime._2
            inferenceTime = runInference(sc, outputRootPath, modelAndTrainingTime._1, vocs, numTopics, totalIter, burnInIter, alpha, beta, alphaAS, testDocs)
        } else if (numHeldOutDocs == 0) { // Don't sample test docs from all the docs
            val trainingDocs = readDocsFromTxt(sc, docsPath, numTerms)
            val modelAndTrainingTime = runTraining(sc, outputRootPath, vocs, numTopics, totalIter, burnInIter, alpha, beta, alphaAS, trainingDocs)
            trainingTime = modelAndTrainingTime._2
            if (args.length > 11) {
                val testDocsPath = args(11)
                val testDocs = readDocsFromTxt(sc, testDocsPath, numTerms)
                inferenceTime = runInference(sc, outputRootPath, modelAndTrainingTime._1, vocs, numTopics, totalIter, burnInIter, alpha, beta, alphaAS, testDocs)
            }
        }

        val appEndedTime = System.currentTimeMillis()
        println("-----------------------------Training time consumed: %f s"
                    .format(trainingTime))
        println("-----------------------------Inference time consumed: %f s"
                    .format(inferenceTime))
        println("-----------------------------Total time consumed: %f s"
                    .format((appEndedTime - appStartedTime) / 1e3))
    }
}

