name := "ticksloader"
scalaVersion := "2.12.4"
version := "1.0"

resolvers ++= Seq(
  "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"
)

libraryDependencies ++= Seq(
  "com.datastax.oss" % "java-driver-core" % "4.0.1",
  "com.typesafe.akka" %% "akka-actor" % "2.5.22",
  //"ch.qos.logback" % "logback-classic" % "1.2.3",
  //"com.typesafe.akka" %% "akka-slf4j" % "2.5.23",
  "ch.qos.logback" % "logback-classic" % "1.3.0-alpha4",
  "org.scala-lang" % "scala-library" % "2.12.4"
)


/*
libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-http-spray-json" % akkaHttpVersion,
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-http" % akkaHttpVersion,
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "com.typesafe.akka" %% "akka-cluster" % akkaVersion,
  "com.typesafe.akka" %% "akka-cluster-tools" % akkaVersion
)
*/

/*
enablePlugins(JavaAppPackaging)
enablePlugins(DockerPlugin)
enablePlugins(AshScriptPlugin)
*/

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case "logback.xml" => MergeStrategy.last
  case "resources/logback.xml" => MergeStrategy.last
  case "resources/application.conf" => MergeStrategy.last
  case "application.conf" => MergeStrategy.last
  case PathList("reference.conf") => MergeStrategy.concat
  case x => MergeStrategy.first
}

assemblyJarName in assembly :="ticksloader.jar"
mainClass in (Compile, packageBin) := Some("ticksloader.TicksLoaderMain")
mainClass in (Compile, run) := Some("ticksloader.TicksLoaderMain")
