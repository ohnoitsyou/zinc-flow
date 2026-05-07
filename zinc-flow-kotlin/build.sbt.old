ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / javacOptions ++= Seq("--release", "25")

lazy val root = (project in file("."))
  .settings(
    name := "zinc-flow-java",
    // Java-only project — drop Scala path suffixes and don't pull the
    // Scala library into the app.
    crossPaths := false,
    autoScalaLibrary := false,
    Compile / mainClass := Some("zincflow.Main"),
    assembly / mainClass := Some("zincflow.Main"),
    // Fork `sbt run` into its own JVM so the worker's non-daemon Jetty
    // threads keep the process alive after main() returns. Without this,
    // sbt's in-process run behavior can tear the worker down as soon as
    // the main method completes. Matters for external trialists running
    // `sbt 'run config.yaml'` — zinc CLI is unaffected either way.
    run / fork := true,
    assembly / assemblyJarName := "zinc-flow.jar",
    // Jetty, Jackson, SnakeYAML, slf4j, kotlin-stdlib all ship their
    // own module-info.class — discard duplicates in the fat jar.
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "versions", _, "module-info.class") => MergeStrategy.discard
      case PathList("module-info.class") => MergeStrategy.discard
      case other =>
        val defaultStrategy = (assembly / assemblyMergeStrategy).value
        defaultStrategy(other)
    },
    libraryDependencies ++= Seq(
      // HTTP — Javalin 6 (minimal-API style, Jetty 12 underneath).
      "io.javalin" % "javalin" % "6.3.0",
      // JSON + CSV + YAML — Jackson everywhere, consistent codec shapes.
      "com.fasterxml.jackson.core" % "jackson-databind" % "2.18.0",
      "com.fasterxml.jackson.datatype" % "jackson-datatype-jsr310" % "2.18.0",
      "com.fasterxml.jackson.dataformat" % "jackson-dataformat-csv" % "2.18.0",
      // Avro — apache.avro for schema, binary, and OCF (Object Container File).
      "org.apache.avro" % "avro" % "1.12.0",
      // Expression language — Apache Commons JEXL 3 (variables, arithmetic,
      // string ops, function namespaces). Used by EvaluateExpression and
      // TransformRecord — no hand-rolled parser.
      "org.apache.commons" % "commons-jexl3" % "3.4.0",
      // Record querying — Jayway JsonPath (RFC 9535 adjacent). Used by
      // QueryRecord to filter records with JsonPath predicates.
      "com.jayway.jsonpath" % "json-path" % "2.9.0",
      // YAML — SnakeYAML for config.yaml parsing.
      "org.yaml" % "snakeyaml" % "2.3",
      // Logging — slf4j API + Logback backend.
      "org.slf4j" % "slf4j-api" % "2.0.16",
      "ch.qos.logback" % "logback-classic" % "1.5.12",
      // Metrics — Micrometer core + Prometheus registry.
      "io.micrometer" % "micrometer-core" % "1.14.2",
      "io.micrometer" % "micrometer-registry-prometheus" % "1.14.2",
      // JUnit 6 — official sbt-jupiter-interface starter shape.
      "com.github.sbt.junit" % "jupiter-interface" % JupiterKeys.jupiterVersion.value % Test,
      "org.junit.jupiter" % "junit-jupiter" % "6.0.3" % Test,
      "org.junit.platform" % "junit-platform-launcher" % "6.0.3" % Test
    ),
    testOptions += Tests.Argument(jupiterTestFramework, "--display-mode=tree")
  )
