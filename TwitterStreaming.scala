// Finding the Top 5 Hashtags every 10 seconds over Window of 30 seconds
//spark-submit --class main.scala.abc.TwitterStreaming --packages "org.apache.spark:spark-streaming-twitter_2.10:1.5.1"  /Users/rahulgulati/Desktop/ScalaProjects/learning-spark/target/scala-2.10/learning-spark_2.10-0.0.1.jar
package main.scala.abc
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._

case class Twitter(tags:String,count:Int)

object TwitterStreaming {
  
  def main(agrs:Array[String])
  {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Seconds(10))
    val sqlcc = new org.apache.spark.sql.SQLContext(sc)
    import sqlcc.implicits._
   
  System.setProperty("twitter4j.oauth.consumerKey", "l7ldXqxePzaksMOGsPobuv0Xm")
  System.setProperty("twitter4j.oauth.consumerSecret", "loMG32QYWLuIBZlGblLFSGSz94qYEFNAYFD1Bkw0wtPSDT0jlF")
  System.setProperty("twitter4j.oauth.accessToken", "1674500270-OgpmSUSEUJRXY2GejjnVLmDZoPjSCzzPG12Ypr1")
  System.setProperty("twitter4j.oauth.accessTokenSecret", "smcQCZTF3BXcwMeXL1Rn4fDFMCZEZJuyEf7bjvZNMNQ41")
  val stream = TwitterUtils.createStream(ssc, None)
  val tags = stream.flatMap { status =>status.getHashtagEntities.map(_.getText)}

    //tags.print()
    
    ssc.checkpoint("/Users/rahulgulati/checkpoint")
    //tags.print()
    val tagsByCount = tags.countByValue()
    val tagscount = tags.countByValueAndWindow(Seconds(30),Seconds(10)).map(a => (a._2,a._1))
    //tagscount.print()
    
    val tagsnot1 = tagscount.filter(x => x._1 != 1)
    //tagsnot1.print()
    
    val sortedtags = tagsnot1.transform(rdd1 => rdd1.sortByKey())
    val final_output = sortedtags.map(x => (x._2,x._1))
 
final_output.foreachRDD{ rdd =>
      /*val outputTwitterTable = rdd.map(x => Twitter(x(0),x(1).toInt)).toDF()
      outputTwitterTable.registerTempTable("Twitter")
     // sqlcc.sql("select tags,count(1) as cnt from Twitter group by tags order by cnt desc limit 5")
     val joineddataframe = sqlcc.sql("select tags,count from Twitter")
     //joineddataframe.printSchema()
     joineddataframe.map(t => "Tags:" + t(0)).collect().foreach(println)*/
  rdd.toDF().registerTempTable("Twitter")
  sqlcc.sql("select _1 as tags,_2 as cnt from Twitter order by cnt desc limit 5").show()
    }
  
    
   val tweets = stream.filter {t =>
     val tags = t.getText.split(" ").filter(_.startsWith("#")).map(_.toLowerCase)
      // val tags = t.getText.split(" ").map(_.toLowerCase)
     //val tags = t.getHashtagEntities.map(_.getText)
    tags.contains("#bigdata")
      // tags.contains("#TimesNow")
    }
    
  
  // tweets.print()
    
    ssc.start()
    ssc.awaitTermination()
    
  }
  
  
}