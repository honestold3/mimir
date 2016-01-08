organization := "Matruss"

name := "mimir"

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
                  	"Typsafe repo" at "http://repo.typesafe.com/typesafe/releases/"
)

