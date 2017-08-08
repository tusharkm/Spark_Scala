package com.neu.edu.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

/**
  * Created by Tushar on 8/7/2017.
  */

// we could count the value using coutByValue() function, however this function is not scalable and we cannot disribute
// the task. ti distribute the task we use RDD to keep the task scalable
object WordCountOnCluster {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc=new SparkContext("local[1]","WordCount")

    val input=sc.textFile("./book.txt")
     val stopdoc=sc.textFile("./stop-word-list.csv")
    // regular expression that extracts words

   val stopWords=stopdoc.flatMap(x=>x.split(","))
    val broadcastStopWords=sc.broadcast(stopWords.collect.toSet)
    val words=input.flatMap(x=>x.split("\\W+"))

    //converting word to lower case

    val lowercaseWords = words.map(x => x.toLowerCase())

    // removing stop words
    val filteredWord=lowercaseWords.filter(!broadcastStopWords.value.contains(_))
    // creating rdd so that data can be scalable



   val wordCounts = filteredWord.map(x => (x, 1)).reduceByKey( (x,y) => x + y )
    // flip the wordCount(word,CountofWord) tuple to wordCountSorted(CountofWord,word) then sort by count

   val wordCountsSorted = wordCounts.map( x => (x._2, x._1) ).sortByKey()

    // print the result by flipping the value word and count

    for (result <- wordCountsSorted) {
      val count = result._1
      val word = result._2
      println(s"$word: $count")
    }



  }
}
