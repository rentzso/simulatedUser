package insightproject.simulated.user

/**
  * Created by rfrigato on 6/16/17.
  */
import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.entity.{ContentType, StringEntity}

import org.apache.http.util.EntityUtils
import org.apache.log4j.{Logger, PropertyConfigurator}
import play.api.libs.json._

import scala.util.Random

case class GdeltNews(url: String, topics: Seq[String])

object SimulatedUser {
  val apiUrl = sys.env.getOrElse("FLASK_ENDPOINT", "http://localhost:5000")
  val logger = Logger.getLogger("ElasticUser")
  PropertyConfigurator.configure("log4j.properties")

  def main(args: Array[String]): Unit = {
    run(args(0).toBoolean)
  }
  def run(isSimple: Boolean) = {
    var result = getRandom()
    val random = new Random(43)
    var topics = Set(result.topics)
    while (true) {
      logger.info(s"visited ${result.url}")
      logger.info(s"topics ${topics}")
      val recommendations = getRecommendations(result.topics, isSimple)
      val choice = random.nextInt(Math.min(10,recommendations.size))
      result = recommendations(choice)
      topics = topics.union(Set(result.topics))
    }
  }
  def getRecommendations(topics: Seq[String], isSimple: Boolean): Seq[GdeltNews] = {
    val client = HttpClientBuilder.create().build()
    val request = new HttpPost(apiUrl + "/topics")
    val requestEntity = new StringEntity(
      buildJsonRequest(topics, isSimple),
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
  def buildJsonRequest(topics: Seq[String], isSimple: Boolean): String = {
    val jsonTopics = topics.map(s => JsString(s))
    val json = Json.obj(
      "simple" -> JsBoolean(isSimple),
      "topics" -> jsonTopics)
    Json.stringify(json)
  }
}
