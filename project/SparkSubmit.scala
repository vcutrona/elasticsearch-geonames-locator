import sbtsparksubmit.SparkSubmitPlugin.autoImport._

object SparkSubmit {
  lazy val settings =
    SparkSubmitSetting("GeonameIngester",
      Seq("--class", "it.unimib.disco.inside.GeonameIngester")
    )
}