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

/**
  * represents a news article from the GDELT dataset
  *
  * @param url the url of the news article
  * @param newsId the GDELT id
  * @param topics the news article topics
  */
case class GdeltNews(url: String, newsId: String, topics: Seq[String])

/**
  * Creates a set of simulated users
  *
  * Each user starts with a random url (and its topics) and then follow the recommendations (randomly)
  *
  */
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
      val queue = new mutable.Queue[(Future[Option[String]], ApiUserState)]
      val producer = buildProducer()
      for (i <- 0 until numUsers) {
        val random = new Random(1000)
        val apiUserState = ApiUserState(
          i, isSimple, mutable.Set.empty[String],
          producer, topic, random
        )
        val f = Future {
          Some(getRandom())
        }
        queue += Tuple2(f, apiUserState)
      }
      val consumeQueue = consumeFutureQueue[Option[String], ApiUserState](newRecommendationOption)
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

  /**
    *
    * @param userId user identifier
    * @param isSimple true if the user is receiving simple recommendations from Elasticsearch
    *                 false if she is receiving the custom recommendations
    * @param topics the list of topics the user visited
    * @param producer the Kafka Producer used to publish statistics for this user
    * @param kafkaTopic the topic to which the producer is publishing
    * @param random random number generator
    */
  case class ApiUserState(userId: Int, isSimple: Boolean,
                            topics: mutable.Set[String],
                            producer: Producer[String, Array[Byte]],
                            kafkaTopic: String, random: Random
                           )

  /**
    *
    * @param response option that wraps a response for recommendations
    * @param apiUserState keeps state of the API user
    * @return a Future wrapping a new recommendation
    */
  def newRecommendationOption(response: Option[String], apiUserState: ApiUserState) = {
    response match {
      case Some(s) => newRecommendation(s, apiUserState)
      case None => throw new Exception("http call failed")
    }
  }

  /**
    *
    * @param response the response to a request for recommendations
    * @param apiUserState keeps state of the API user
    * @return a Future wrapping a new recommendation
    */
  def newRecommendation(response: String, apiUserState: ApiUserState): Future[Option[String]] = {
    val json = Json.parse(response)
    val recommendations = (json \ "recommendations").as[Seq[JsObject]].map(
      jsObject => GdeltNews(
        (jsObject \ "url").as[String],
        (jsObject \ "id").as[String],
        (jsObject \ "topics").as[Seq[String]]
      )
    )
    // how much time it took to execute the query within Elasticsearch
    val took = (json \ "took").as[Int]
    val choice = apiUserState.random.nextInt(Math.min(10, recommendations.size))
    val result = recommendations(choice)
    val initialTopicsSize = apiUserState.topics.size
    apiUserState.topics ++= result.topics
    logger.info(s"User ${apiUserState.userId} visited ${result.url}")
    logger.info(s"topics ${apiUserState.topics}")
    val avroRecord = UserStats2Avro.encode(
      apiUserState.userId, apiUserState.topics.size,
      apiUserState.topics.size - initialTopicsSize,
      apiUserState.isSimple, result.newsId, result.url,
      took
    )
    if (apiUserState.topics.size > 40) {
      apiUserState.topics.clear()
      Future {
        Some(getRandom())
      }
    } else {
      val key: String = apiUserState.userId + " " + apiUserState.isSimple
      apiUserState.producer.send(new ProducerRecord(
        apiUserState.kafkaTopic,
        key, avroRecord
      ))
      getRecommendation(apiUserState)
    }

  }

  /**
    *
    * @param apiUserState the current user state
    * @return a Future wrapping recommendations
    */
  def getRecommendation(apiUserState: ApiUserState): Future[Option[String]] = {
    val request = url(apiUrl +  "/topics").POST
    val body = buildJsonBody(apiUserState)
    val jsonRequest = request.setContentType("application/json", "UTF-8") << body
    Http(jsonRequest OK as.String).option
  }

  /**
    *
    * @return a new Kafka Producer
    */
  def buildProducer(): KafkaProducer[String, Array[Byte]] = {
    val props = new Properties()
    props.put("bootstrap.servers", sys.env.getOrElse("BOOTSTRAP_SERVERS", "localhost:9092"))
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
    props.put("batch.size", "10")
    new KafkaProducer[String, Array[Byte]](props)
  }

  /**
    *
    * @return a random news url
    */
  def getRandom(): String = {
    val client = HttpClientBuilder.create().build()
    val request = new HttpGet(apiUrl + "/random")
    val response = client.execute(request)
    EntityUtils.toString(response.getEntity())
  }

  /**
    *
    * @param apiUserState the current user state
    * @return the body of a request for recommendations
    */
  def buildJsonBody(apiUserState: ApiUserState): String = {
    val jsonTopics = apiUserState.topics.toList.map(JsString(_))
    val json = Json.obj(
      "userId" -> JsNumber(apiUserState.userId),
      "simple" -> JsBoolean(apiUserState.isSimple),
      "topics" -> jsonTopics
    )
    Json.stringify(json)
  }
}
