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
import scala.io.Source
import breeze.linalg.{DenseVector => BDV, SparseVector => BSV}
import org.apache.spark.Logging
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.linalg.{Vectors, DenseVector => SDV, SparseVector => SSV}
import org.apache.spark.rdd.RDD

object LDAUtil {
    def readTrainingAndHeldOutDocsFromTxt(sc: SparkContext,
                        docsPath: String,
                        numTerms: Int,
                        numHeldOutDocs: Int): (RDD[(Long, SSV)], RDD[(Long, SSV)]) = {
        assert(numHeldOutDocs > 0)
        //val rawDocs = sc.parallelize(sc.textFile(docsPath).takeSample(false, 1000000))
        val rawDocs = sc.textFile(docsPath)
        val heldOutDocs = sc.parallelize(rawDocs.takeSample(false, numHeldOutDocs))
        val trainingDocs = rawDocs.subtract(heldOutDocs)

        (convertDocsToBagOfWords(sc, trainingDocs, numTerms),
         convertDocsToBagOfWords(sc, heldOutDocs, numTerms))
    }

    def readDocsFromTxt(sc: SparkContext,
                        docsPath: String,
                        numTerms: Int): RDD[(Long, SSV)] = {
        val rawDocs = sc.textFile(docsPath)
        //val rawDocs = sc.parallelize(sc.textFile(docsPath).takeSample(false, 1000000))
        convertDocsToBagOfWords(sc, rawDocs, numTerms)
    }

    def convertDocsToBagOfWords(sc: SparkContext,
                                rawDocs: RDD[String],
                                numTerms: Int): RDD[(Long, SSV)] = {
            rawDocs.map(line => line.split(' ')).map(tokens => {
                val docId = tokens(0).toLong
                val docTermCount = BSV.zeros[Double](numTerms)
                (1 until tokens.length).map { i =>
                    val termCountPair = tokens(i).split(':')
                    val termId = termCountPair(0).toInt
                    val termCount = termCountPair(1).toDouble
                    docTermCount(termId) += termCount
                }

                (docId, Vectors.fromBreeze(docTermCount).asInstanceOf[SSV])
            })
    }

    def generateOutputPath(model: LDAModel, resultPath: String, totalIter: Int, burnInIter: Int): String = {
      val resultPathWithParams = resultPath + "LDAModel_%.2f_%.2f_%.2f_%.2f_%d_%d_".format(model.alpha, model.beta, model.alphaAS, model.perplexity, totalIter, burnInIter)

      resultPathWithParams
    }

    def outputToLocal(outputPath: String,
                      outputMethod: Any => Array[String],
                      outputObject: Any): Unit = {
        val printWriter = new PrintWriter(new File(outputPath))
        outputMethod(outputObject).foreach(printWriter.println)
        printWriter.close()
    }

    def outputModel(sc: SparkContext,
                    model: LDAModel,
                    vocabulary: Array[String],
                    resultPath: String): Unit = {
      val topicCounterPath = resultPath + "TopicCounters.txt"
      val topicTermCountPath = resultPath + "TopicTermCounts.txt"
      val topicTermProbPath = resultPath + "TopicTermProbs.txt"

      val globalTopicCounterWithIndex = model.asInstanceOf[LDAModel].globalTopicCounter.zipWithIndex

      //output globalTopicCounter order by count desc
      def topicCounterOutputMethod = (globalTopicCounter: Any) => {
          globalTopicCounter.asInstanceOf[Array[(Double, Int)]].sortWith(_._1 > _._1).map { case (count, index) =>
            "%d %.2f".format(index, count)
          }
      }

      outputToLocal(topicCounterPath, topicCounterOutputMethod, globalTopicCounterWithIndex)

      val topicTermCountMatrix = transpose(model.asInstanceOf[LDAModel].termTopicCounter)
      val topicTermCountMatrixWithIndex = topicTermCountMatrix.zipWithIndex

      def topicTermOutputMethod = (precision: Int, topicTermMatrixWithIndex: Any) => {
          val template = "%s:%." + precision + "f"

          topicTermMatrixWithIndex.asInstanceOf[Array[(Array[Double], Int)]].map { case (termDistribution, topicIndex) =>
            //Filter out terms with 0 value, then sort by value desc
            val row = "%d %s".format(topicIndex,
                            termDistribution.zipWithIndex.filter(_._1 > 0).sortWith(_._1 > _._1).map { case (termCount, termIndex) =>
                                template.format(vocabulary(termIndex), termCount)
                            }.mkString(" ")
                        )
            row
          }
      }

      outputToLocal(topicTermCountPath, topicTermOutputMethod(2, _), topicTermCountMatrixWithIndex)

      val topicTermProbMatrixWithIndex = topicTermCountMatrixWithIndex.map { case (termDistribution, topicIndex) =>
        val probDistribution = termDistribution.map(count => (count * 1.0F / globalTopicCounterWithIndex(topicIndex)._1))

        (probDistribution, topicIndex)
      }

      outputToLocal(topicTermProbPath, topicTermOutputMethod(8, _), topicTermProbMatrixWithIndex)
      //Below method is used to output from RDD to HDFS
      /*
      val topicTermCounterWriter = new PrintWriter(new File(topicTermCounterPath))
      topicTermMatrixWithIndex.map { case (termDistribution, topicIndex) =>
        val row = "%d %s".format(topicIndex,
                        termDistribution.zipWithIndex.map { case (termProb, termIndex) =>
                          "%s:%.10f".format(vocabulary(termIndex), termProb)
                        }.mkString(" ")
                    )
        row
      }.foreach(topicTermCounterWriter.println)

      topicTermCounterWriter.close()
*/
/*
      sc.parallelize(model.globalTopicCounter.zipWithIndex).map { case (count, index) =>
          "%d %f".format(index, count)
      }.coalesce(1).saveAsTextFile(topicCounterPath)

      sc.parallelize(model.topicTermCounter.zipWithIndex).map { case (arr, index) =>
        val ssv = arr.asInstanceOf[SSV]
        val bsv = new BSV[Double](ssv.indices, ssv.values, ssv.size)
        val termCountsStr = bsv.activeIterator.map(t => "%s:%f".format(vocabulary(t._1), t._2)).mkString(" ")
      }.saveAsTextFile(topicTermCounterPath)
      */
    }

    def outputTestResults(sc: SparkContext,
                          docTopicDist: RDD[(Long, SSV)],
                          resultPath: String): Unit = {
        val testResultOutputPath = resultPath + "TestResults.txt" 
        def testResultOutputMethod = (docTopicDist: Any) => {
                docTopicDist.asInstanceOf[RDD[(Long, SSV)]].collect.zipWithIndex.map { case(pair, index) => {
                val docId = pair._1
                val ssv = pair._2.asInstanceOf[SSV]
                //Sort by topic probability desc
                val topicDistStr = ssv.toArray.zipWithIndex.filter(_._1 > 0).sortWith(_._1 > _._1).map(
                                    t => "%d:%f".format(t._2, t._1)).mkString(" ")
                "%d %d %s".format(index, docId, topicDistStr)
                }
            }.toArray
        }

        outputToLocal(testResultOutputPath, testResultOutputMethod, docTopicDist)

        //The code below is to output to HDFS
        /*
        docTopicDist.zipWithIndex.map { case(sv, index) =>
            val ssv = sv.asInstanceOf[SSV]
            val bsv = new BSV[Double](ssv.indices, ssv.values, ssv.size)
            val topicDistStr = bsv.activeIterator.map(
                                t => "%s:%f".format(t._1, t._2)).mkString(" ")
            "%d %s".format(index, topicDistStr)
        }.coalesce(1).saveAsTextFile(testResultOutputPath)
        */
    }

    def transpose(ttc: Array[Array[Double]]): Array[Array[Double]] = {
        val numTerms = ttc.length
        val numTopics = ttc(0).length
        val matrix = Array.ofDim[Double](numTopics, numTerms)
        for (j <- 0 until (matrix(0).length)) {
            val topicDist = ttc(j)
            for (i <- 0 until (matrix.length)) {
                matrix(i)(j) = topicDist(i)
            }
        }
        
        matrix
    }

    def restoreParams(modelPath: String): (Double, Double, Double, Double) = {
        val modelParams = modelPath.split('/').last.split('_')
        val alpha = modelParams(1).toDouble
        val beta = modelParams(2).toDouble
        val alphaAS = modelParams(3).toDouble
        val perplexity = modelParams(4).toDouble

        (alpha, beta, alphaAS, perplexity)
    }

    def restoreTopicCounter(topicCounterPath: String, numTopics: Int): BDV[Double] = {
        val topicCounterText = Source.fromFile(topicCounterPath).getLines
        val topicCounter = BDV.zeros[Double](numTopics)
        for (row <- topicCounterText) {
            val pair = row.split(' ') 
            val topicId = pair(0).toInt
            assert(topicId >= 0)
            val topicCount = pair(1).toDouble
            topicCounter(topicId) = topicCount
        }

        topicCounter
    }

    def restoreTermTopicCounter(topicTermClusterPath: String,
                                vocabulary: Array[String],
                                numTopics: Int,
                                numTerms: Int): Array[BSV[Double]] = {
        val topicTermClusterText = Source.fromFile(topicTermClusterPath).getLines
        val termTopicCounter = new Array[BSV[Double]](numTerms).map( bsv => BSV.zeros[Double](numTopics) )

        for (row <- topicTermClusterText) {
            val tokens = row.split(' ')
            val topicId = tokens(0).toInt
            for (i <- 1 until tokens.length) {
                val termCountPair = tokens(i).split(':')
                val term = termCountPair(0)
                val termId = vocabulary.indexOf(term)
                if (-1 < termId) {  //If the term exists in current vocabulary
                    val count = termCountPair(1).toDouble
                    termTopicCounter(termId)(topicId) = count
                }
            }
        }

        termTopicCounter
    }

    def restoreModel(sc: SparkContext,
                     modelPath: String,
                     vocabulary: Array[String],
                     numTopics: Int,
                     numTerms: Int): LDAModel = {
        val params = restoreParams(modelPath)

        val topicCounterPath = modelPath + "_TopicCounters.txt"
        val topicTermClusterPath = modelPath + "_TopicTermCounts.txt"
        val topicCounter = restoreTopicCounter(topicCounterPath, numTopics)

        val termTopicCounter = restoreTermTopicCounter(
                                topicTermClusterPath, vocabulary, numTopics, numTerms)

        new LDAModel(topicCounter, termTopicCounter, params._1, params._2, params._3, params._4)
    }
}
