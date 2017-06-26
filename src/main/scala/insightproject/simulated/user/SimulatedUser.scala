package insightproject.simulated.user

/**
  * Created by rfrigato on 6/16/17.
  */
import java.util.Properties

import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.entity.{ContentType, StringEntity}
import org.apache.http.util.EntityUtils
import org.apache.log4j.{Level, Logger, PropertyConfigurator}
import play.api.libs.json._

import scala.collection.JavaConversions._
import scala.util.Random
import org.apache.kafka.clients.producer._

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import dispatch._
import Defaults._

import scala.collection.mutable


case class GdeltNews(url: String, newsId: String, topics: Seq[String])

object SimulatedUser {
  val apiUrl = sys.env.getOrElse("FLASK_ENDPOINT", "http://localhost")
  val logger = Logger.getLogger("ElasticUser")
  PropertyConfigurator.configure("log4j.properties")
  logger.setLevel(Level.toLevel(sys.env.getOrElse("LOG4J_LEVEL", "INFO")))
  def main(args: Array[String]): Unit = {
    val numUsers =
      if (args.size > 2) args(2).toInt
      else 1
    if (args.size >= 2) {
      val isSimple = args(0).toBoolean
      val topic = args(1)
      val queue = new mutable.Queue[(Future[Option[String]], AdditionalData)]
      val producer = buildProducer()
      for (i <- 0 until numUsers) {
        val random = new Random(1000)
        val additionalData = AdditionalData(
          i, isSimple, mutable.Set.empty[String],
          producer, topic, random
        )
        val f = Future {
          Some(getRandom())
        }
        queue += Tuple2(f, additionalData)
      }
      val consumeQueue = consumeFutureQueue[Option[String], AdditionalData](newRecommendationOption)
      var future: Future[Option[String]] = consumeQueue(queue)
      val batchSize = sys.env.getOrElse("BATCH_SIZE_SIMULATED_USER", "10").toInt
      val timeout = sys.env.getOrElse("TIMEOUT_SIMULATED_USER", "90").toInt
      while (queue.size > 0) {
        for (i <- 0 until batchSize) {
          future = consumeQueue(queue)
        }
        Await.result(future, timeout seconds)
      }
    } else {
      println(
        """Usage:
          |java -cp simulatedUser-assembly-1.0.jar insightproject.simulated.user.SimulatedUser \
          |<true|false> <topic> <numUsers(optional)>
        """.stripMargin)
    }
  }
  case class AdditionalData(userId: Int, isSimple: Boolean,
                            topics: mutable.Set[String],
                            producer: Producer[String, Array[Byte]],
                            kafkaTopic: String, random: Random
                           )
  def newRecommendationOption(response: Option[String], additionalData: AdditionalData) = {
    response match {
      case Some(s) => newRecommendation(s, additionalData)
      case None => throw new Exception("http call failed")
    }
  }
  def newRecommendation(response: String, additionalData: AdditionalData): Future[Option[String]] = {
    val json = Json.parse(response)
    val recommendations = (json \ "recommendations").as[Seq[JsObject]].map(
      jsObject => GdeltNews(
        (jsObject \ "url").as[String],
        (jsObject \ "id").as[String],
        (jsObject \ "topics").as[Seq[String]]
      )
    )
    val took = (json \ "took").as[Int]
    val choice = additionalData.random.nextInt(Math.min(10, recommendations.size))
    val result = recommendations(choice)
    val initialTopicsSize = additionalData.topics.size
    additionalData.topics ++= result.topics
    logger.info(s"User ${additionalData.userId} visited ${result.url}")
    logger.info(s"topics ${additionalData.topics}")
    val avroRecord = UserStats2Avro.encode(
      additionalData.userId, additionalData.topics.size,
      additionalData.topics.size - initialTopicsSize,
      additionalData.isSimple, result.newsId, result.url,
      took
    )
    if (additionalData.topics.size > 40) {
      additionalData.topics.clear()
      Future {
        Some(getRandom())
      }
    } else {
      val key: String = additionalData.userId + " " + additionalData.isSimple
      additionalData.producer.send(new ProducerRecord(
        additionalData.kafkaTopic,
        key, avroRecord
      ))
      getRecommendation(additionalData)
    }

  }
  def getRecommendation(additionalData: AdditionalData): Future[Option[String]] = {
    val request = url(apiUrl +  "/topics").POST
    val body = buildJsonBody(additionalData)
    val jsonRequest = request.setContentType("application/json", "UTF-8") << body
    Http(jsonRequest OK as.String).option
  }
  def buildProducer(): KafkaProducer[String, Array[Byte]] = {
    val props = new Properties()
    props.put("bootstrap.servers", sys.env.getOrElse("BOOTSTRAP_SERVERS", "localhost:9092"))
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
    props.put("batch.size", "10")
    new KafkaProducer[String, Array[Byte]](props)
  }
  def getRandom(): String = {
    val client = HttpClientBuilder.create().build()
    val request = new HttpGet(apiUrl + "/random")
    val response = client.execute(request)
    EntityUtils.toString(response.getEntity())
  }
  def buildJsonBody(additionalData: AdditionalData): String = {
    val jsonTopics = additionalData.topics.toList.map(JsString(_))
    val json = Json.obj(
      "userId" -> JsNumber(additionalData.userId),
      "simple" -> JsBoolean(additionalData.isSimple),
      "topics" -> jsonTopics
    )
    Json.stringify(json)
  }
}
