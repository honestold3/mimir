import sbt._
import Keys._

object ContexoBuild extends Build
{
	lazy val mimir = Project(id = "mimir", base = file(".")) dependsOn(crawler, classifier);

	lazy val crawler = Project(id = "crawler", base = file("crawler"));

  lazy val classifier = Project(id = "classifier", base = file("classifier"));

  lazy val probe = Project(id = "probe", base = file("probe"));

}
