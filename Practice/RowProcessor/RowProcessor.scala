import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object ProcessorTest {
    def main(args: Array[String]) {
        //val logFile = "file:///home/spark/workspace/quinzh/src/scala/RowProcessor/test.txt"
        val logFile = "quinzh/test.txt"
        val conf = new SparkConf().setAppName("Processor Test")
        val sc = new SparkContext(conf)
        val logData = sc.textFile(logFile, 2).cache()
        val filteredData = logData.map(line=>processLine(line))
        //filteredData.saveAsTextFile("file:///home/spark/workspace/quinzh/src/scala/RowProcessor/testOutput.txt")
        filteredData.saveAsTextFile("quinzh/testOutput")
    }

    def processLine(line: String): (Array[String]) = {
        val pair = line.split(',')
        if (pair.length >= 3) {
            Array(pair(0), pair(1), pair(2))
        } else {
            Array(null, null, null)
        }
    }
}
