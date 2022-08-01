package ai.economicdatasciences.sia.model

import java.time.Instant

case class Post(
  commentCount:Option[Int],
  lastActivityDate:Option[Instant],
  ownerUserId:Option[Long],
  body:String,
  score:Option[Int],
  creationDate:Option[Instant],
  viewCount:Option[Int],
  title:String,
  tags:String,
  answerCount:Option[Int],
  acceptedAnswerId:Option[Long],
  postTypeId:Option[Long],
  id:Long
)

object StringImplicits {
   implicit class StringImprovements(val s: String) {
      import scala.util.control.Exception.catching
      def toIntSafe: Option[Int] = catching(classOf[NumberFormatException]) opt s.toInt
      def toLongSafe: Option[Long] = catching(classOf[NumberFormatException]) opt s.toLong
      // def toTimestampSafe = catching(classOf[IllegalArgumentException]) opt Timestamp.valueOf(s)
      def toInstantSafe: Option[Instant] = catching(classOf[IllegalArgumentException]) opt Instant.parse((s + "Z").replace(" ","T"))
   }
}
