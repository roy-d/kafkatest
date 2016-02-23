package lab


import java.io.File
import java.net.InetSocketAddress
import java.nio.file.Files

import kafka.utils.{Logging, CoreUtils}
import org.apache.zookeeper.server.{NIOServerCnxnFactory, ZooKeeperServer}

object EmbeddedZK extends Logging{
  private val snapshotDir = tempDir()
  private val logDir = tempDir()

  private val tickTime = 500
  private val zookeeper = new ZooKeeperServer(snapshotDir, logDir, tickTime)
  private val factory = new NIOServerCnxnFactory()
  private val addr = new InetSocketAddress("127.0.0.1", 0)
  factory.configure(addr, 0)
  factory.startup(zookeeper)
  private val port = zookeeper.getClientPort
  val zkConnect: String = s"127.0.0.1:$port"
  if(zookeeper.isRunning) logger.debug("ZK started")

  sys.addShutdownHook({
    CoreUtils.swallow(zookeeper.shutdown())
    CoreUtils.swallow(factory.shutdown())
    CoreUtils.rm(logDir)
    CoreUtils.rm(snapshotDir)
    logger.debug("ZK shutdown")
  })

  def tempDir() = {
    val ioTmpDir = System.getProperty("java.io.tmpdir")
    val parentFile = new File(ioTmpDir)
    parentFile.mkdirs()

    val f = Files.createTempDirectory(parentFile.toPath, "kafka-it-").toFile
    f.deleteOnExit()
    sys.addShutdownHook(CoreUtils.rm(f))
    f
  }
}
