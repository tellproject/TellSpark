package ch.ethz.queries

/**
  * Handles query parameters
  */
object ParamHandler {

  var st = "192.168.0.21:7241"
  var cm = "192.168.0.21:7242"
  var partNum = 4
  var qryNum = 0
  var chunkSizeSmall = 0x100000L  // 1MB
  var chunkSizeBig = 0x100000000L // 4GB
  var chunkSizeMedium = 0x80000000L // 2GB
  var parallelScans = 6

  def getParams(args: Array[String]) = {
    // client properties
    if (args.length >= 3 && args.length <= 8) {
      st = args(0)
      cm = args(1)
      partNum = args(2).toInt
      if (args.length > 3) {
        qryNum = args(3).toInt
      }
      if (args.length > 4) {
        chunkSizeSmall = args(4).toLong
      }
      if (args.length > 5) {
        chunkSizeMedium = args(5).toLong
      }
      if (args.length > 6) {
        chunkSizeBig = args(6).toLong
      }
      if (args.length > 7) {
        parallelScans = args(7).toInt
      }
    } else {
      println("[TELL] Incorrect number of parameters")
      println("[TELL] <strMng> <commitMng> <partNum> [<query-num>] [<small-chunk-size>] [<medium-chunk-size] [<big-chunk-size>] [<num-parallel-scans>]")
      throw new RuntimeException("Invalid number of arguments")
    }
  }
}
