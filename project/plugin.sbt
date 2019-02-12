resolvers += Resolver.url("artifactory",
  url("http://scalasbt.artifactoryonline.com/scalasbt/sbt-plugin-releases"))(Resolver.
  ivyStylePatterns)

logLevel := Level.Warn

resolvers += Resolver.url("bintray-sbt-plugin-releases",
  url("https://dl.bintray.com/content/sbt/sbt-plugin-releases"))

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.6")