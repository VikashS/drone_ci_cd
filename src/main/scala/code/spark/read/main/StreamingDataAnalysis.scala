package code.spark.read.main


import scala.concurrent.duration._
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.{SQLContext, SaveMode}

object StreamingDataAnalysis {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sparkConf = new SparkConf().setAppName("TwitterDataAnalysis")
    sparkConf.setIfMissing("spark.master", "local[5]")
    sparkConf.setIfMissing("spark.cassandra.connection.host", "127.0.0.1")
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    val kafkaTopicRaw = "mytopics"
    val kafkaBroker = "127.0.01:9092"
    val searchValue = args.mkString("")
    val broadcastSearchValue = ssc.sparkContext.broadcast(searchValue)
    val sqlContext = new SQLContext(ssc.sparkContext
    )
    import sqlContext.implicits._


    val topics: Set[String] = kafkaTopicRaw.split(",").map(_.trim).toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> kafkaBroker)
    val rawTwitterStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics).map(_._2)

    val hashTags: DStream[String] = rawTwitterStream.flatMap(_.split(",")).filter(_.contains(broadcastSearchValue.value))
    val topTenPopularTwiite: DStream[(Int, String)] = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(10))
      .map { case (topic, count) => (count, topic) }
      .transform(_.sortByKey(false))

    val counyVal = topTenPopularTwiite.foreachRDD(rdd => {
      val topList = rdd.take(10)
      val df= rdd.toDF("count","tags")
        //df.show(20,false)
        .write.mode(SaveMode.Append)
        .format("org.apache.spark.sql.cassandra")
        .options(Map( "table" -> "toptentag", "keyspace" -> "vkspace"))
        .save()
    })
    //ssc.checkpoint("addd the hdfs path from cloudera vm  hdfs://ip:8020/test")
    rawTwitterStream.print
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}