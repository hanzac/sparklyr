package sparklyr

object Shell {
  private[this] var backend: Backend = null

  def main(args: Array[String]): Unit = {
    if (args.length > 4 || args.length < 2) {
      System.err.println(
        "Usage: Backend port id [--service] [--remote]\n" +
        "  port:      port the gateway will listen to\n" +
        "  id:        arbitrary numeric identifier for this backend session\n" +
        "  --service: prevents closing the connection from closing the backen\n" +
        "  --remote:  allows the gateway to accept remote connections\n"
      )

      System.exit(-1)
    }

    val port = args(0).toInt
    val sessionId = args(1).toInt
    val isService = args.contains("--service")
    val isRemote = args.contains("--remote")

    backend = new Backend()
    backend.setType(isService, isRemote, false)
    backend.init(port, sessionId)
  }

  def getBackend(): Backend = {
    backend
  }
}
