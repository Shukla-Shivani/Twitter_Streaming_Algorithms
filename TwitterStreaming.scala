import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j.Status
import scala.util.Random.{nextFloat, nextInt}

object TwitterStreaming {

  val consumer_key = "8tyAPkmFXQRSRPV9ixOdmNVjd"
  val consumer_secret = "3k8nbLloQhT9r5pl5x8nwVYEdfGvbxShkhZ8jtaqucCbARw7y0"
  val key = "794908099693793280-wXoD1jELXiHIyxsEbJPrk7mR15hK8Ar"
  val secret = "8gD3xJRllG2rbyJclKeGc1kWjSM7xWH3E4lVrtCEaN7Gg"
  var current_reservoir_length = 0
  val reservoir_max_length = 100
  var total_tweets_length = 0
  var master_hashtag_counter = collection.mutable.Map[String, Int]()

  var reservoir_status_length = Array[Int]()
  var reservoir_hashtag_counter = Array[collection.mutable.Map[String, Int]]()

  def onStatus(status: Status): Unit = {
    current_reservoir_length += 1
    if (current_reservoir_length <= reservoir_max_length) {
      val hashtags = status.getHashtagEntities.map(
        hashtag => hashtag.getText
      )

      val hashtag_counter = collection.mutable.Map(
        hashtags.groupBy(identity).mapValues(_.length).toSeq: _*
      )

      master_hashtag_counter = master_hashtag_counter ++ hashtag_counter.map{
        case (k,v) => k -> (v + master_hashtag_counter.getOrElse(k,0))
      }

      val tweet_length = status.getText.size
      total_tweets_length += tweet_length

      reservoir_status_length :+= tweet_length
      reservoir_hashtag_counter :+= hashtag_counter
    } else {
      val probability = reservoir_max_length.toFloat / current_reservoir_length.toFloat
      if (probability > nextFloat()) {
        val random_element_index = nextInt(reservoir_max_length)
        val popped_element_length = reservoir_status_length(random_element_index)
        var popped_element_counter = reservoir_hashtag_counter(random_element_index)

        val hashtags = status.getHashtagEntities.map(
          hashtag => hashtag.getText
        )

        val hashtag_counter = collection.mutable.Map(
          hashtags.groupBy(identity).mapValues(_.length).toSeq: _*
        )

        master_hashtag_counter = master_hashtag_counter ++ hashtag_counter.map{
          case (k,v) => k -> (v + master_hashtag_counter.getOrElse(k,0))
        }

        master_hashtag_counter = master_hashtag_counter ++ popped_element_counter.map{
          case (k,v) => k -> (-v + master_hashtag_counter.getOrElse(k,0))
        }

        val tweet_length = status.getText.size
        reservoir_hashtag_counter(random_element_index) = hashtag_counter
        reservoir_status_length(random_element_index) = tweet_length

        total_tweets_length += tweet_length - popped_element_length

        println("The number of the twitter from beginning: " + current_reservoir_length)
        println("Top 5 hot hashtags")
        master_hashtag_counter.toSeq.sortBy(-_._2).take(5).foreach(
          tuple => println(tuple._1+":"+tuple._2)
        )

        println("The average length of the twitter is: " + total_tweets_length.toFloat/reservoir_max_length.toFloat)
        println("\n\n")
      }
    }

  }

  def main(args: Array[String]): Unit = {
    System.setProperty("twitter4j.oauth.consumerKey", consumer_key)
    System.setProperty("twitter4j.oauth.consumerSecret", consumer_secret)
    System.setProperty("twitter4j.oauth.accessToken", key)
    System.setProperty("twitter4j.oauth.accessTokenSecret", secret)

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf()
    conf.setAppName("TwitterStreaming")
    conf.setMaster("local[2]")

    val ssc = new StreamingContext(conf, Seconds(10))
    ssc.sparkContext.setLogLevel("ERROR")
    val filters = Array("#")
    val stream = TwitterUtils.createStream(ssc, None, filters)

    stream.foreachRDD(
      (status_rdd, time) => status_rdd.collect().foreach(status => onStatus(status))
    )

    ssc.start()
    ssc.awaitTermination()

  }
}


