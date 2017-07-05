package insightproject.simulated.user

/**
  * Created by rfrigato on 6/22/17.
  */
import com.twitter.bijection.Injection
import com.twitter.bijection.avro.GenericAvroCodecs
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}

/**
  * Defines the methods used to create Avro messages with user statistics
  */
object UserStats2Avro {
  /**
    * Creates the Avro schema used to create the Avro record
    */
  val userStatsAvroSchema = {
    val parser = new Schema.Parser
    val schemaFile = getClass().getResourceAsStream("/avroSchemas/user-stats-avro-schema.json")
    parser.parse(schemaFile)
  }
  /**
    * Converts an Avro record into Bytes
    */
  val recordInjection : Injection[GenericRecord, Array[Byte]] =
    GenericAvroCodecs.toBinary[GenericRecord](userStatsAvroSchema)

  /**
    * Encodes user statistics for one click event
    *
    * @param userId id of the user
    * @param numUserTopics number of topics favorited by the user
    * @param newUserTopics number of new topics the user was exposed
    * @param isSimple true if the user is exposed to the standard recommendation system
    * @param newsId id of this click news
    * @param newsUrl url of this click news
    * @param took time spent by elasticsearch to send the next recommendation
    * @return a byte encoded message with the user click statistics
    */
  def encode(userId: Int, numUserTopics: Int, newUserTopics: Int,
             isSimple: Boolean, newsId: String, newsUrl: String, took: Int
            ) = {
    val avroRecord = new GenericData.Record(userStatsAvroSchema)
    avroRecord.put("user_id", userId)
    avroRecord.put("num_user_topics", numUserTopics)
    avroRecord.put("num_new_user_topics", newUserTopics)
    avroRecord.put("news_id", newsId)
    avroRecord.put("news_url", newsUrl)
    avroRecord.put("score_type", isSimple.toString)
    val timestamp = System.currentTimeMillis
    avroRecord.put("timestamp", timestamp)
    avroRecord.put("took", took)
    recordInjection(avroRecord)
  }
}

