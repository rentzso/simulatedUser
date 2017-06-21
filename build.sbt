name := "simulatedUser"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.kafka" % "kafka_2.11" % "0.10.2.1"
libraryDependencies ++= Seq(
  "org.apache.logging.log4j" % "log4j-core" % "2.7",
  "org.apache.logging.log4j" % "log4j-api" % "2.7"
)
libraryDependencies += "com.typesafe.play" % "play-json_2.11" % "2.5.15"
mainClass in assembly := Some("insightproject.simulated.user.SimulatedUser")
// https://mvnrepository.com/artifact/org.apache.httpcomponents/httpclient
libraryDependencies += "org.apache.httpcomponents" % "httpclient" % "4.5.3"
