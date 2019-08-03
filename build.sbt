/*
 * Copyright 2019 Radicalbit S.r.l.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

addCommandAlias("fixCheck", "; compile:scalafix --check ; test:scalafix --check")
addCommandAlias("fix", "; compile:scalafix ; test:scalafix")

lazy val `nsdb-kafka-connect` = (project in file("."))
  .settings(PublishSettings.settings: _*)
  .enablePlugins(AutomateHeaderPlugin)
  .settings(LicenseHeader.settings: _*)
  .settings(
    organization := "io.radicalbit.nsdb",
    name := "nsdb-kafka-connect",
    scalaVersion := "2.12.7",
    crossScalaVersions := Seq("2.11.12", "2.12.7"),
    version := "0.8.0-SNAPSHOT",
    scalacOptions := Seq(
      "-Ypartial-unification",
      "-Ywarn-unused",
      "-Ywarn-unused-import",
      "-Ywarn-dead-code",
      "-Ywarn-numeric-widen",
      "-unchecked",
      "-deprecation",
      "-encoding",
      "utf8"
    )
  )
  .settings(libraryDependencies ++= Dependencies.libraries)
  .settings(
    resolvers in ThisBuild ++= Seq(
      "Radicalbit Public Releases" at "https://tools.radicalbit.io/artifactory/public-release/",
      "Radicalbit Public Snapshots" at "https://tools.radicalbit.io/artifactory/public-snapshot/",
      Resolver.mavenLocal
    )
  )
  .settings(
    addCompilerPlugin(scalafixSemanticdb),
    scalafmtOnCompile in ThisBuild := true,
    run in Compile := Defaults.runTask(fullClasspath in Compile,
                                       mainClass in (Compile, run),
                                       runner in (Compile, run)),
    // some options to facilitate tests
    fork in Test := true,
    parallelExecution in Test := false,
    testForkedParallel in Test := false,
    javaOptions in Test += "-Xms2048m",
    javaOptions in Test += "-Xmx4096m",
    javaOptions in Test += "-XX:ReservedCodeCacheSize=256m",
    javaOptions in Test += "-XX:MaxMetaspaceSize=512m"
  )
  .settings(
    assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = true),
    artifact in (Compile, assembly) := {
      val art: Artifact = (artifact in (Compile, assembly)).value
      art.withClassifier(Some(""))
    },
    addArtifact(artifact in (Compile, assembly), assembly),
    test in assembly := {},
    assemblyMergeStrategy in assembly := {
      case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.first
      case PathList("CHANGELOG.adoc")                           => MergeStrategy.first
      case PathList("CHANGELOG.html")                           => MergeStrategy.first
      case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
    }
  )
