package kz.talgat

import akka.actor.ActorSystem
import akka.stream.{ClosedShape, IOResult}
import akka.stream.scaladsl.{Broadcast, FileIO, Flow, GraphDSL, RunnableGraph, Sink, Source}
import akka.util.ByteString
import spray.json._

import java.nio.file.Paths
import java.time.format.DateTimeFormatter
import java.time.ZonedDateTime
import scala.concurrent.Future


// ------------------ Life Codding ------------- Not solved
//Read a provided file using Akka Streams.
//Perform the following aggregations:
//The total sum of transactions (amount) for each userId.
//The number of transactions for each userId.
//The total sum of transactions grouped by time buckets (split data into hourly intervals).
//Print the results to the console.
// ------------- Take 50 minutes ----------------------
/// -------------Out -----------------
//Hour 2024-01-01T21:00Z total sum: 168.21
//Hour 2024-01-01T08:00Z total sum: 24.67
//Hour 2024-01-01T01:00Z total sum: 488.09
//User 2 sum: 488.09 and count: 1
//Hour 2024-01-01T02:00Z total sum: 335.18
//User 10 sum: 335.18 and count: 1
//Hour 2024-01-01T20:00Z total sum: 25.86
//User 1 sum: 25.86 and count: 1
//User 5 sum: 192.88 and count: 2

object GameDevs extends App {
  case class Transaction(userId: Int, amount: Double, timestamp: ZonedDateTime)

  implicit object ZonedDateTimeFormat extends JsonFormat[ZonedDateTime] {
    private val formatter = DateTimeFormatter.ISO_ZONED_DATE_TIME

    def write(obj: ZonedDateTime): JsValue = JsString(formatter.format(obj))

    def read(json: JsValue): ZonedDateTime = json match {
      case JsString(s) => ZonedDateTime.parse(s, formatter)
      case _ => throw DeserializationException("Not correct Json")
    }
  }

  import spray.json.DefaultJsonProtocol._

  implicit val transactionFormat: RootJsonFormat[Transaction] = jsonFormat3(Transaction.apply)


  implicit val system: ActorSystem = ActorSystem("Test")

  val filePath: String = "transactions.json"

  val fileSource: Source[ByteString, Future[IOResult]] = FileIO.fromPath(Paths.get(filePath))

  val source = fileSource
    .fold(ByteString.empty)(_ ++ _)
    .map(_.utf8String.parseJson.convertTo[List[Transaction]])
    .mapConcat(identity)

  // User Sums
  val userSumsAndCount = Flow[Transaction]
    .groupBy(100, _.userId)
    .fold((0, 0.0, 0)) { case ((_, sum, count), tran) =>
      (tran.userId, sum + tran.amount, count + 1)
    }
    .mergeSubstreams

  val userSumAndCountSink = Sink.foreach[(Int, Double, Int)] { case (userId, sum, count) =>
    println(s"User $userId sum: $sum and count: $count")
  }

  // Total Transactions By Hour
  val totalTransactionsByHour = Flow[Transaction]
    .groupBy(100, t => t.timestamp.withMinute(0).withSecond(0))
    .fold(("", 0.0)) { case ((_, total), tran) =>
      (tran.timestamp.withMinute(0).withSecond(0).toString, total + tran.amount)
    }
    .mergeSubstreams

  val totalTransactionsByHourSink = Sink.foreach[(String, Double)] { case (hourStr, total) =>
    println(s"Hour $hourStr total sum: $total")
  }

  // Graph: Broadcast to all three aggregations
  val graph = RunnableGraph.fromGraph(GraphDSL.create() { implicit builder =>
    import GraphDSL.Implicits._

    val broadcast = builder.add(Broadcast[Transaction](2))

    val src = builder.add(source)

    src ~> broadcast.in

    broadcast.out(0) ~> userSumsAndCount ~> userSumAndCountSink
    broadcast.out(1) ~> totalTransactionsByHour ~> totalTransactionsByHourSink

    ClosedShape
  })

  graph.run()
}
