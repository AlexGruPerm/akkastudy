package com.example

import akka.actor.{Actor, ActorSystem, Props}

//*************************************************************************************************

object PrintMyActorRefActor {
  def props: Props =
    Props(new PrintMyActorRefActor)
}

class PrintMyActorRefActor extends Actor {
  override def receive: Receive = {
    case "printit" =>
      val secondRef = context.actorOf(Props.empty, "second-actor")
      println(s"Second: $secondRef")
    case s:String if (s == "msg") => println("Message = ["+s+"]")
  }
}

//*************************************************************************************************

object ActorHierarchyExperiments extends App {
  val system = ActorSystem("testSystem")

  val firstRef = system.actorOf(PrintMyActorRefActor.props, "first-actor")
  println("First: "+ firstRef)
  firstRef ! "printit"
  firstRef ! "msg"

  Thread.sleep(1000)
  println("-----------------")

  val secRef = system.actorOf(PrintMyActorRefActor.props, "second-actor")
  println(s"Second: "+ secRef)
  secRef ! "msg"

  Thread.sleep(1000)
  system.terminate()
}
