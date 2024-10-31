package kz.talgat

import akka.actor.ActorSystem

object SecondExample  extends App {
  implicit val system = ActorSystem()
  import system.dispatcher

  //sink to fold
  val praphCount = LearnStream.praphCount.run()
  praphCount.foreach(e => println(s"Rows will be proceed $e"))

  //sink to file
  val resultGraph = LearnStream.graph.run()
  resultGraph._1.foreach(b => println(s"Bytes from file was read $b"))
  resultGraph._2.foreach(b => println(s"Bytes to file was added $b"))
}
