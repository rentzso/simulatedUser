name := "simulatedUser"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.kafka" % "kafka_2.11" % "0.10.2.1"
libraryDependencies ++= Seq(
  "org.apache.logging.log4j" % "log4j-core" % "2.7",
  "org.apache.logging.log4j" % "log4j-api" % "2.7"
)
libraryDependencies += "com.typesafe.play" % "play-json_2.11" % "2.5.15"

libraryDependencies += "org.apache.httpcomponents" % "httpclient" % "4.5.3"
libraryDependencies += "joda-time" % "joda-time" % "2.8.1"
libraryDependencies += "org.apache.avro" % "avro" % "1.8.2"
libraryDependencies += "com.twitter" % "bijection-avro_2.11" % "0.9.5"
libraryDependencies += "net.databinder.dispatch" %% "dispatch-core" % "0.11.2"

mainClass in assembly := Some("insightproject.simulated.user.SimulatedUser")
