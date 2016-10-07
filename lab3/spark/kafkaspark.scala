import java.util.HashMap

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import kafka.serializer.{DefaultDecoder, StringDecoder}

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel

object KafkaWordCount {
  def main(args: Array[String]) {

    val kafkaConf = Map(
    	"metadata.broker.list" -> "localhost:9092",
    	"zookeeper.connect" -> "localhost:2181",
    	"group.id" -> "kafka-spark-streaming",
    	"zookeeper.connection.timeout.ms" -> "1000")

    val sparkConf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("checkpoint")

    // if you want to try the receiver-less approach, comment the below line and uncomment the next one
    val messages = KafkaUtils.createStream[String, String, DefaultDecoder, StringDecoder](ssc, kafkaConf, Map("avg" -> 1), StorageLevel.MEMORY_ONLY)
    // val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaConf, Set("avg"))

    val values = messages.map(pair => (pair._2.split(",")(0), (1, pair._2.split(",")(1).toInt)))
    val pairs = values.reduceByKey((p1, p2) => (p1._1 + p2._1, p1._2 + p2._2))

    // State is the sum and the count of elements
    def mappingFunc(key: String, value: Option[(Int, Int)], state: State[(Int, Int)]): Option[(String, Double)] = {
      val current = state.getOption.getOrElse(0, 0)
      val updatedValue = value.getOrElse(0, 0)
    	val updated = (current._1 + updatedValue._1, current._2 + updatedValue._2) // Update the sum and no of elements
      val output = (key, updated._2.toDouble / updated._1.toDouble) // Calculate the average
      state.update(updated)
      Some(output)
    }

    val stateDstream = pairs.mapWithState(StateSpec.function(mappingFunc _))

    stateDstream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
