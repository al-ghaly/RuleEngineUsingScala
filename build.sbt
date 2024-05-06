// Define global settings for the build
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "3.3.3"

// Define the root project
lazy val root = (project in file("."))
    .settings(
        name := "Final Project",

        // Add library dependencies here within the project settings
        libraryDependencies += "com.oracle.database.jdbc" % "ojdbc8" % "19.3.0.0"
    )
