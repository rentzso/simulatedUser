package insightproject.simulated.user

/**
  * Created by rfrigato on 6/16/17.
  */
import java.util.Properties

import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.entity.{ContentType, StringEntity}
import org.apache.http.util.EntityUtils
import org.apache.log4j.{Logger, PropertyConfigurator}
import play.api.libs.json._
import scala.collection.JavaConversions._
import scala.util.Random
import org.apache.kafka.clients.producer._

case class GdeltNews(url: String, topics: Seq[String])

object SimulatedUser {
  val apiUrl = sys.env.getOrElse("FLASK_ENDPOINT", "http://localhost")
  val logger = Logger.getLogger("ElasticUser")
  PropertyConfigurator.configure("log4j.properties")

  def main(args: Array[String]): Unit = {
    if (args.size == 2) {
      run(0, args(0).toBoolean, args(1))
    } else if (args.size > 2) {
      val isSimple = args(0).toBoolean
      val topic = args(1)
      val numUsers = args(2).toInt
      val tasks = 0 until numUsers map(
        i => task({run(i, isSimple, topic)})
        )
      tasks.foreach(_.join())
    } else {
      println(
        """Usage:
          |java -cp simulatedUser-assembly-1.0.jar insightproject.simulated.user.SimulatedUser \
          |<true|false> <numUsers(optional)>
        """.stripMargin)
    }

  }
  def run(userId: Int, isSimple: Boolean, kafkaTopic: String) = {
    var result = getRandom()
    val random = new Random(43)
    var topics = result.topics.toSet
    val producer = buildProducer()
    while (true) {
      logger.info(s"User $userId visited ${result.url}")
      logger.info(s"topics ${topics}")
      val sample = Random.shuffle(topics).take(10).toList
      val recommendations = getRecommendations(userId, sample, isSimple)
      val choice = random.nextInt(Math.min(10, recommendations.size))
      result = recommendations(choice)
      topics = topics.union(result.topics.toSet)
      val avroRecord = UserStats2Avro.encode(userId, topics.size, isSimple)
      producer.send(new ProducerRecord(
        kafkaTopic,
        isSimple.toString,
        avroRecord
      ))
    }
  }
  def buildProducer(): KafkaProducer[String, Array[Byte]] = {
    val props = new Properties()
    props.put("bootstrap.servers", sys.env.getOrElse("BOOTSTRAP_SERVERS", "localhost:9092"))
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
    new KafkaProducer[String, Array[Byte]](props)
  }
  def getRecommendations(userId: Int, topics: Seq[String], isSimple: Boolean): Seq[GdeltNews] = {
    val client = HttpClientBuilder.create().build()
    val request = new HttpPost(apiUrl + "/topics")
    val requestEntity = new StringEntity(
      buildJsonRequest(userId, topics, isSimple),
      ContentType.APPLICATION_JSON)
    request.setEntity(requestEntity)
    val response = client.execute(request)
    val jsonResponse = EntityUtils.toString(response.getEntity())
    val json = Json.parse(jsonResponse)
    (json \ "recommendations").as[Seq[JsObject]].map(
      jsObject => GdeltNews(
        (jsObject \ "url").as[String],
        (jsObject \ "topics").as[Seq[String]]
      )
    )
  }
  def getRandom(): GdeltNews = {
    val client = HttpClientBuilder.create().build()
    val request = new HttpGet(apiUrl + "/random")
    val response = client.execute(request)
    val jsonResponse = EntityUtils.toString(response.getEntity())
    val json = Json.parse(jsonResponse)
    GdeltNews(
      (json \ "url").as[String],
      (json \ "topics").as[Seq[String]]
    )
  }
  def buildJsonRequest(userId: Int, topics: Seq[String], isSimple: Boolean): String = {
    val jsonTopics = topics.map(s => JsString(s))
    val json = Json.obj(
      "userId" -> JsNumber(userId),
      "simple" -> JsBoolean(isSimple),
      "topics" -> jsonTopics)
    Json.stringify(json)
  }
}
