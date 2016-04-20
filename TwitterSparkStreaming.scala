package main.scala.abc
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._

case class Twitter(tags:String,count:Int)

object TwitterSparkStreaming {
  
  def main(agrs:Array[String])
  {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Seconds(10))
    val sqlcc = new org.apache.spark.sql.SQLContext(sc)
    import sqlcc.implicits._
   
  System.setProperty("twitter4j.oauth.consumerKey", "")
  System.setProperty("twitter4j.oauth.consumerSecret", "")
  System.setProperty("twitter4j.oauth.accessToken", "")
  System.setProperty("twitter4j.oauth.accessTokenSecret", "")
  val stream = TwitterUtils.createStream(ssc, None)
  val tags = stream.flatMap { status =>status.getHashtagEntities.map(_.getText)}
    
    ssc.checkpoint("/Users/rahulgulati/checkpoint")
    val tagsByCount = tags.countByValue()
    val tagscount = tags.countByValueAndWindow(Seconds(30),Seconds(10)).map(a => (a._2,a._1))
    
    val tagsnot1 = tagscount.filter(x => x._1 != 1)
  
    
    val sortedtags = tagsnot1.transform(rdd1 => rdd1.sortByKey())
    val final_output = sortedtags.map(x => (x._2,x._1))
 
final_output.foreachRDD{ rdd =>
  rdd.toDF().registerTempTable("Twitter")
  sqlcc.sql("select _1 as tags,_2 as cnt from Twitter order by cnt desc limit 5").show()
    }
  
    
   val tweets = stream.filter {t =>
     val tags = t.getText.split(" ").filter(_.startsWith("#")).map(_.toLowerCase)
    tags.contains("#bigdata")
    }
    
    
    ssc.start()
    ssc.awaitTermination()
    
  }
  
  
}
}