addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.1")

resolvers += Resolver.url("artifactory", url("http://scalasbt.artifactoryonline.com/scalasbt/sbt-plugin-releases"))(Resolver.ivyStylePatterns)
