// sbt-assembly produces a fat jar on `zinc build` (sbt assembly).
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.3.1")

// sbt-jupiter-interface provides `JupiterKeys.jupiterVersion.value`
// and the `jupiterTestFramework` value used in build.sbt — the
// official JUnit 5/6 starter shape.
addSbtPlugin("com.github.sbt.junit" % "sbt-jupiter-interface" % "0.18.0")
