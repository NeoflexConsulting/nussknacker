package pl.touk.nussknacker.engine.api

import java.nio.charset.StandardCharsets
import io.circe
import io.circe.{ACursor, Decoder, Encoder, HCursor, Json, KeyEncoder}
import io.circe.generic.extras.Configuration

import java.net.URI
import scala.annotation.tailrec
import scala.jdk.CollectionConverters.mapAsScalaMapConverter

object CirceUtil {

  implicit val configuration: Configuration = Configuration
    .default
    .withDefaults
    .withDiscriminator("type")


  def decodeJson[T:Decoder](json: String): Either[circe.Error, T]
    = io.circe.parser.parse(json).right.flatMap(Decoder[T].decodeJson)

  def decodeJson[T:Decoder](json: Array[Byte]): Either[circe.Error, T] = decodeJson(new String(json, StandardCharsets.UTF_8))

  def decodeJsonUnsafe[T:Decoder](json: String, message: String): T = unsafe(decodeJson(json), message)

  def decodeJsonUnsafe[T:Decoder](json: Array[Byte]): T = unsafe(decodeJson(json), "")

  private def unsafe[T](result: Either[circe.Error, T], message: String) = result match {
    case Left(error) => throw DecodingError(s"Failed to decode - $message, error: ${error.getMessage}", error)
    case Right(data) => data
  }

  case class DecodingError(message: String, ex: Throwable) extends IllegalArgumentException(message, ex)

  object codecs {

    implicit def jMapEncoder[K: KeyEncoder, V: Encoder]: Encoder[java.util.Map[K, V]] = Encoder[Map[K, V]].contramap(_.asScala.toMap)
    implicit lazy val uriDecoder: Decoder[URI] = Decoder.decodeString.map(URI.create)
    implicit lazy val uriEncoder: Encoder[URI] = Encoder.encodeString.contramap(_.toString)

  }

  implicit class RichACursor(cursor: ACursor) {

    def downAt(p: Json=> Boolean): ACursor = {
      @tailrec
      def go(c: ACursor): ACursor = c match {
        case success: HCursor => if (p(success.value)) success else go(success.right)
        case other            => other
      }

      go(cursor.downArray)
    }

  }

}
