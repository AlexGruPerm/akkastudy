package com.example

import akka.actor.{Actor, ActorSystem, Props}
import akka.event.Logging
import com.example.TestFirstActor.system

class MySecond extends Actor {
  val log = Logging(context.system, this)

  def receive = {
    case "test" => log.info("received test")
    case "stop" => context.stop(self)
    case "areyouhere" => log.info("second YES.")
    case _ => log.info("received unknown message")
  }

  override def preStart(): Unit =
    log.info("preStart event.")


  override def postStop(): Unit =
    log.info("postStop event.")

}

class MyActor extends Actor {
  val log = Logging(context.system, this)

  def receive = {
    case "test" => log.info("received test")
    case "stop" => context.stop(self)
    case "create" => {
      log.info("received create")
      val sa = system.actorOf(Props[MySecond],"secondActor")
    }
    case "create5" => {
      log.info("received create5")
      1.to(5).by(1).foreach(elm => system.actorOf(Props[MySecond],"secondActor"+elm))
    }
    case "countchild" => {
      log.info("received countchild")
      val childs = context.children.toList
      log.info("Founded childs:"+childs.length)
    }

    case "snd" => {
      log.info("received snd")
      //val sa = context.actorSelection("akka://actorsSystem/user/secondActor")
      //sa ! "test"
    }

    case "sendtoall" => {
      system.actorSelection("/user/secondActor*") ! "areyouhere"
    }

    case "areyouhere" => log.info("first YES.")

    case _ => log.info("received unknown message")
  }


  override def preStart(): Unit =
    log.info("preStart event.")


  override def postStop(): Unit =
    log.info("postStop event.")

}

/**
  *
  * Exception in thread "main" akka.actor.ActorInitializationException:
  * You cannot create an instance of [com.example.MyActor] explicitly using the constructor (new).
  * You have to use one of the 'actorOf' factory methods to create a new actor.
  *
*/
object TestFirstActor extends App {
  val system = ActorSystem("actorsSystem")
  val fa = system.actorOf(Props[MyActor],"firstActor")
  fa ! "go"
  fa ! "test"
  //fa ! "create"
  fa ! "snd"
  fa ! "create5"
  fa ! "countchild"
  fa ! "sendtoall"

}
