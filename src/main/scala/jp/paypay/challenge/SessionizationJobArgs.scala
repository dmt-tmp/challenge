package jp.paypay.challenge

import scala.concurrent.duration._
import scopt.OptionParser

case class SessionizationJobArgs(
  accessLogEntriesPath: String = "",
  baseOutputDirectory: String = "",
  nbOutputFiles: Int = -1,
  newSessionThreshold: Duration = 0.minute
)

object SessionizationJobArgs {
  val parser = new OptionParser[SessionizationJobArgs]("SessionizationJob") {
    opt[String]("access-logs-path")
      .required()
      .validate(value =>
        if (value.isEmpty) failure("Parameter access-logs-path musn't be empty")
        else success
      )
      .action { (value, args) => args.copy(accessLogEntriesPath = value) }
      .text("Path to a file or folder containing access log entries, that can be read by 'spark.read.csv'")

    opt[String]("output-dir")
      .required()
      .validate(value =>
        if (value.isEmpty) failure("Parameter output-dir musn't be empty")
        else success
      )
      .action{ (value, args) => args.copy(baseOutputDirectory = value) }
      .text("Folder where sessionized logs will be saved")

    opt[Int]("nb-output-files")
      .required()
      .validate(value =>
        if (value >= 1) success
        else failure("Paramter nb-output-files must be greater than or equal to 1")
      )
      .action{ (value, args) => args.copy(nbOutputFiles = value) }
      .text("Number of files that will be written in directory 'output-dir'")

    opt[Duration]("new-session-threshold")
      .required()
      .validate(value =>
        if(value > 0.second) success
        else failure("new-session-threshold must be greater than 0")
      )
      .action { (value, args) => args.copy(newSessionThreshold = value) }
      .text("""Duration used to detect new sessions, e.g. '15minutes'.
          |If a user is inactive for 'new-session-threshold' or more, any future activity is attributed to a new session
        """.stripMargin)
  }
}