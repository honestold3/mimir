organization := "Matruss"

name := "classifier"

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
                  	"SnakeYAML repository "at "http://oss.sonatype.org/content/groups/public/"
)

libraryDependencies ++= Seq(
        "log4j" % "log4j" % "1.2.15" excludeAll(
                                         ExclusionRule(organization = "com.sun.jdmk"),
                                         ExclusionRule(organization = "com.sun.jmx"),
                                         ExclusionRule(organization = "javax.jms")
        ),
		"org.apache.hadoop" % "hadoop-common" % "2.0.0-cdh4.1.1",
		"org.apache.hadoop" % "hadoop-mapreduce-client-app" % "2.0.0-cdh4.1.1",
        "com.github.scopt" %% "scopt" % "2.1.0",
        "com.codahale" % "jerkson_2.9.1" % "0.5.0",
        "org.apache.mrunit" % "mrunit" % "0.9.0-incubating" classifier "hadoop2",
        "org.scalatest" %% "scalatest" % "1.8" % "test"
)

mainClass := Some("org.matruss.mimir.classifier.FCRClassifier")

excludeFilter in unmanagedJars := "avro-tools-1.6.1.jar"

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
