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

object SampleDocs extends App with Logging {
    def sample(sc: SparkContext,
                docs: RDD[String],
                numSampleDocs: Int): RDD[String] = {
        assert(numSampleDocs > 0)
        sc.parallelize(docs.takeSample(false, numSampleDocs))
    }

    def selectDocs(docs: RDD[String], docIds: Array[String]): RDD[String] = {
        docs.filter(line => docIds.contains(line.split(' ')(0)))
    }

    override def main(args: Array[String]) {
        val sampleDocsNum = args(0).toInt
        val docsPath = args(1)
        val outputPath = args(2)

        assert(sampleDocsNum >= 0)
        
        val conf = new SparkConf().setAppName("SampleDocs")
        val sc = new SparkContext(conf)

        val docs = sc.textFile(docsPath)
        val sampledDocs = sample(sc, docs, sampleDocsNum)
        if (args.length > 3) {
            val selectedDocIds = args.slice(3, args.length)

            val selectedDocs = selectDocs(docs, selectedDocIds)
            val finalDocs = sampledDocs.union(selectedDocs).distinct()
            finalDocs.coalesce(1).saveAsTextFile(outputPath)

            println("Sampling %d docs (including docId %s) from %s to %s completed.".format(sampleDocsNum, selectedDocIds.mkString(", "), docsPath, outputPath))
        } else {
            sampledDocs.coalesce(1).saveAsTextFile(outputPath)

            println("Sampling %d docs from %s to %s completed.".format(sampleDocsNum, docsPath, outputPath))
        }
    }
}
