import _root_.sbtassembly.Plugin.AssemblyKeys._
import _root_.sbtassembly.Plugin.MergeStrategy
import _root_.sbtassembly.Plugin.PathList
import sbtassembly.Plugin.AssemblyKeys._
import sbtassembly.Plugin.{MergeStrategy, PathList}

assemblySettings

name := "ETL_Tools"

version := "0.2"

scalaVersion := "2.10.5"

test in assembly :={}

scalacOptions ++= Seq("-unchecked", "-Xlint", "-deprecation", "-Yno-adapted-args", "-feature", "-Xfatal-warnings")

fork := true

resolvers += "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-mllib_2.10" % "1.4.1"
)


mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) => {
  case x if Assembly.isConfigFile(x) =>
    MergeStrategy.concat
  case PathList(ps @ _*) if Assembly.isReadme(ps.last) || Assembly.isLicenseFile(ps.last) =>
    MergeStrategy.rename
  case PathList("META-INF", xs @ _*) =>
    (xs map {_.toLowerCase}) match {
      case ("manifest.mf" :: Nil) | ("index.list" :: Nil) | ("dependencies" :: Nil) =>
        MergeStrategy.discard
      case ps @ (x :: xs) if ps.last.endsWith(".sf") || ps.last.endsWith(".dsa") =>
        MergeStrategy.discard
      case "plexus" :: xs =>
        MergeStrategy.discard
      case "services" :: xs =>
        MergeStrategy.filterDistinctLines
      case ("spring.schemas" :: Nil) | ("spring.handlers" :: Nil) =>
        MergeStrategy.filterDistinctLines
      case _ => MergeStrategy.first
    }
  case _ => MergeStrategy.first
}
}