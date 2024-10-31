package kz.talgat

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.{Flow, Sink, Source}

import scala.concurrent.Future

object Main_First {
  def main(args: Array[String]): Unit = {
    //  println("Hello world!")

    implicit val system = ActorSystem()
    import system.dispatcher //thread pool

    //source
    val source = Source(1 to 1000)
    val flow = Flow[Int].map(x => x * 2)
    val sink = Sink.foreach[Int](println)
    val summingSink = Sink.fold[Int, Int](0)((currentSum, incomingElement) => currentSum + incomingElement)
    val graph = source.via(flow).to(sink)

    val anotherGraph = source.via(flow).toMat(sink)((leftJedi, rightJedi) => rightJedi) // (Keep.right)

    //  val value: NotUsed = graph.run() // make the graph come alive

  //  val anotherValue: Future[Done] = anotherGraph.run()
  //  anotherValue.onComplete(_ => println("Stream is Done"))

    val sumFuture: Future[Int] = source.via(flow).toMat(summingSink)((leftJedi, rightJedi) => rightJedi).run() // (Keep.right)
    sumFuture.foreach(println)

    //once you start, no turning back
    // akka streams values = MATERIALIZED VALUES
    // akka streams values may or may not be connected to actual elements that go through the graph
  }
}