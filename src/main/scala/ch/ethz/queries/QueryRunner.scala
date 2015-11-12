package ch.ethz.queries

/**
 * Created by marenato on 12.11.15.
 */
object QueryRunner {

  def main(args : Array[String]) {
    var st = "192.168.0.11:7241"
    var cm = "192.168.0.11:7242"
    var cn = 4
    var cs = 5120000
    var masterUrl = "local[12]"
    var appName = "ch_Qry1"

    // client properties
    if (args.length >= 4) {
      st = args(0)
      cm = args(1)
      cn = args(2).toInt
      cs = args(3).toInt
      if (args.length == 6) {
        masterUrl = args(4)
        appName = args(5)
      } else {
        println("[TELL] Incorrect number of parameters")
        println("[TELL] <strMng> <commitMng> <chunkNum> <chunkSz> <masterUrl> <appName>")
        sys.exit()
      }
    }
    val qry = new Q1
    qry.execute(st, cm, cn, cs, masterUrl, appName)
  }
}
