package kz.talgat

import akka.NotUsed
import akka.stream.IOResult
import akka.stream.scaladsl.{FileIO, Flow, Framing, Keep, RunnableGraph, Sink, Source}
import akka.util.ByteString
import spray.json.RootJsonFormat

import java.nio.charset.StandardCharsets
import java.nio.file.{Path, Paths, StandardOpenOption}
import scala.concurrent.Future
import scala.util.Try

object LearnStream {
  case class Client(name: String, age: Int)
  val transformStringToClient: String => Option[Client] = (line: String) => {
    Try {
      line.split(" ") match {
        case Array(name, age) => Client(name.trim, age.trim.toInt)
      }
    }.toOption
  }

  val pathSourceFile: Path = Paths.get("source.txt")
  val pathSinkFile: Path = Paths.get("sink.txt")

  //source
  val source: Source[ByteString, Future[IOResult]] = FileIO.fromPath(pathSourceFile)

  //flow
  val flowByteStringToString: Flow[ByteString, String, NotUsed] = Framing
    .delimiter(ByteString("\n"), 512, false)
    .map(_.decodeString(StandardCharsets.UTF_8))

  val convertParseStringToClient: Flow[String, Client, NotUsed] = Flow[String]
    .map(transformStringToClient).collect {
      case Some(value) => value
    }

  val filterAgeOfSource: Flow[Client, Client, NotUsed] = Flow[Client]
    .filter(_.age < 25)

  import spray.json.DefaultJsonProtocol._
  implicit val clientJson: RootJsonFormat[Client] = jsonFormat2(Client)

  val flowTramsAndSerialize: Flow[Client, ByteString, NotUsed] = Flow[Client]
    .map(c => ByteString(s"${clientJson.write(c).toString()}\n", StandardCharsets.UTF_8))

  val sink: Sink[ByteString, Future[IOResult]] = FileIO.toPath(pathSinkFile, Set(
    StandardOpenOption.CREATE,
    StandardOpenOption.WRITE,
    StandardOpenOption.APPEND,
  ))

  val sinkCount: Sink[Client, Future[Int]] = Sink.fold[Int, Client](0)((sum, _) => sum + 1)

  val graph: RunnableGraph[(Future[IOResult], Future[IOResult])] = source
    .via(flowByteStringToString)
    .via(convertParseStringToClient)
    .via(filterAgeOfSource)
    .via(flowTramsAndSerialize)
    .toMat(sink)(Keep.both)

  val praphCount = source
    .via(flowByteStringToString)
    .via(convertParseStringToClient)
    .via(filterAgeOfSource)
    .toMat(sinkCount)(Keep.right)

}
