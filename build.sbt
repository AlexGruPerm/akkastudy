name := "ticksloader"
version := "0.1"
scalaVersion := "2.11.8"
version := "1.0"

/**
  *
  * Driver 3:
  * import com.datastax.driver.core.ResultSet
  * import com.datastax.driver.core.Row
  * import com.datastax.driver.core.SimpleStatement
  *
  * Driver 4.1:
  * import com.datastax.oss.driver.api.core.cql.ResultSet;
  * import com.datastax.oss.driver.api.core.cql.Row;
  * import com.datastax.oss.driver.api.core.cql.SimpleStatement;
  *
*/

libraryDependencies ++= Seq(
  "com.typesafe" % "config" % "1.3.4",
  "com.datastax.oss" % "java-driver-core" % "4.0.1",
  "com.typesafe.akka" %% "akka-actor" % "2.5.22",
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "org.scala-lang" % "scala-library" % "2.11.8",
  "com.madhukaraphatak" %% "java-sizeof" % "0.1",
  "org.scalatest" %% "scalatest" % "3.0.5" % Test
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case "logback.xml" => MergeStrategy.last
  case "resources/logback.xml" => MergeStrategy.last
  case "resources/application.conf" => MergeStrategy.last
  case "application.conf" => MergeStrategy.last
  case x => MergeStrategy.first
}

assemblyJarName in assembly :="ticksloader.jar"
mainClass in (Compile, packageBin) := Some("ticksloader.TicksLoaderMain")
mainClass in (Compile, run) := Some("ticksloader.TicksLoaderMain")
