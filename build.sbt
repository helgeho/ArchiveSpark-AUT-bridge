
name := "ArchiveSparkAUTbridge"

version := "0.1"

scalaVersion := "2.11.12"

resolvers += Resolver.mavenLocal

libraryDependencies += "io.archivesunleashed" % "aut" % "0.13.1-SNAPSHOT" % "provided"

libraryDependencies += "com.github.helgeho" %% "archivespark" % "2.7.6" % "provided"

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}