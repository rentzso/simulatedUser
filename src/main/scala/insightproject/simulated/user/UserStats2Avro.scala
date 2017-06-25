package insightproject.simulated.user

/**
  * Created by rfrigato on 6/22/17.
  */
import com.twitter.bijection.Injection
import com.twitter.bijection.avro.GenericAvroCodecs
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}

/**
  * Created by rfrigato on 6/22/17.
  */
object UserStats2Avro {
  val userStatsAvroSchema = {
    val parser = new Schema.Parser
    val schemaFile = getClass().getResourceAsStream("/avroSchemas/user-stats-avro-schema.json")
    parser.parse(schemaFile)
  }
  val recordInjection : Injection[GenericRecord, Array[Byte]] =
    GenericAvroCodecs.toBinary[GenericRecord](userStatsAvroSchema)
  def encode(userId: Int, numUserTopics: Int, newUserTopics: Int,
             isSimple: Boolean, newsId: String, newsUrl: String
            ) = {
    val avroRecord = new GenericData.Record(userStatsAvroSchema)
    avroRecord.put("user_id", userId)
    avroRecord.put("num_user_topics", numUserTopics)
    avroRecord.put("num_new_user_topics", newUserTopics)
    avroRecord.put("news_id", newsId)
    avroRecord.put("news_url", newsUrl)
    avroRecord.put("score_type", isSimple.toString)
    recordInjection(avroRecord)
  }
}

