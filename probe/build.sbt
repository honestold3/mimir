organization := "Matruss"

name := "probe"

version := "0.1"

logLevel := Level.Info

scalaVersion := "2.9.2"

scalacOptions ++= Seq("-deprecation", "-unchecked")

parallelExecution in (Test,assembly) := false

initialize ~= { _ => System.setProperty("slf4j.version","1.6.1")}

resolvers ++= Seq(
                    "snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
                    "releases"  at "http://oss.sonatype.org/content/repositories/releases",
		  			"cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
                  	"codahale repo" at "http://repo.codahale.com/",
                  	"JBoss repository" at "http://repository.jboss.org/maven2/",
                  	"Typsafe repo" at "http://repo.typesafe.com/typesafe/releases/",
                  	"spray repo" at "http://repo.spray.io"
)

libraryDependencies ++= Seq(
		"com.typesafe.akka" % "akka-actor" % "2.0.4",
		"org.apache.httpcomponents" % "httpclient" % "4.2.3",
		"org.apache.hadoop" % "hadoop-common" % "2.0.0-cdh4.1.1",
        "com.github.scopt" %% "scopt" % "2.1.0",
		"io.spray" % "spray-can" % "1.0-M5" % "test",
		"com.typesafe.akka" % "akka-testkit" % "2.0.3" % "test",
        "org.scalatest" %% "scalatest" % "1.8" % "test"
)

mainClass := Some("org.matruss.mimir.probe.WebProbe")

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) => {
    case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
    case PathList("org", "apache", "commons", xs @ _*) => MergeStrategy.first
    case PathList("org", "hamcrest", xs @ _*) => MergeStrategy.first
    case PathList("org", "jboss", "netty", xs @ _*) => MergeStrategy.first
    case PathList("META-INF", xs @ _*) =>
    (xs map {_.toLowerCase}) match {
      case ("manifest.mf" :: Nil) |
           ("index.list" :: Nil) |
           ("dependencies" :: Nil) |
           ("notice.txt" :: Nil) |
           ("notice" :: Nil) |
           ("license.txt" :: Nil)|
           ("license" :: Nil) => MergeStrategy.discard
      case ps @ (x :: xs) if ps.last.endsWith(".sf") || ps.last.endsWith(".dsa") =>
        MergeStrategy.discard
      case "plexus" :: xs =>
        MergeStrategy.discard
      case "services" :: xs =>
        MergeStrategy.filterDistinctLines
      case ("spring.schemas" :: Nil) | ("spring.handlers" :: Nil) =>
        MergeStrategy.filterDistinctLines
      case ("jboss-beans.xml" :: Nil) =>
        MergeStrategy.first
      case _ => MergeStrategy.deduplicate
    }
    case "application.conf" => MergeStrategy.concat
    case "unwanted.txt"     => MergeStrategy.discard
    case x => old(x)
  }
}
