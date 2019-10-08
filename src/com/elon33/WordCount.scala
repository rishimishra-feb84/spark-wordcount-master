package com.elon33

import org.apache.spark._
import org.apache.spark.SparkContext._

object wordcount {
    def main(args: Array[String]) {
     val conf = new SparkConf.setAppName("WordCount").setMaster("local")
    val sc = new SparkContext(conf)

    //Read input file
    val input = sc.textFile("input.txt")

    input.flatMap { line => line.split(" ")
    } //for each line split the line into words.
      .map { word => (word, 1)
      } //for each word create a key/value pair, with the word as key and 1 as value

      .reduceByKey(_ + _) //Sum values with same key
      .saveAsTextFile("outputfile") //Save result to a text file

    //StoppingSpark context
    sc.stop()
    }
}
