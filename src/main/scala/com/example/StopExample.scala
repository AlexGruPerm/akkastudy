package com.example

import akka.actor.{Actor, ActorSystem, Props}


//*************************************************************************************************

object StartStopActor1 {
  def props: Props = Props(new StartStopActor1)
}

class StartStopActor1 extends Actor {
  override def preStart(): Unit = {
    println("first started")
                    context.actorOf(StartStopActor2.props, "second")
    val secondRef = context.actorOf(Props.empty, "second-actor")

  }
  override def postStop(): Unit = println("first stopped")

  override def receive: Receive = {
    case "stop" => context.stop(self)
  }
}

//*************************************************************************************************
/**
  * Props is a configuration object using in creating an Actor; it is immutable, so it is thread-safe and fully shareable.
  *
  * Examples on Scala API:
  *
  * val props = Props.empty
  * val props = Props[MyActor]
  * val props = Props(classOf[MyActor], arg1, arg2)
  *
  * val otherProps = props.withDispatcher("dispatcher-id")
  * val otherProps = props.withDeploy(<deployment info>)
  *
* */
object StartStopActor2 {
  def props: Props = Props(new StartStopActor2)
}

class StartStopActor2 extends Actor {
  override def preStart(): Unit = println("second started")
  override def postStop(): Unit = println("second stopped")

  // Actor.emptyBehavior is a useful placeholder when we don't
  // want to handle any messages in the actor.
  override def receive: Receive = Actor.emptyBehavior
}

//*************************************************************************************************

object StopExample extends App {
  val system = ActorSystem("stopSystem")
  val first = system.actorOf(StartStopActor1.props, "first")
  first ! "stop"
}
