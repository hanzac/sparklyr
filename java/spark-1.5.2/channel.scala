package sparklyr

import java.net.{InetAddress, InetSocketAddress}
import java.util.concurrent.TimeUnit

import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.{ChannelFuture, ChannelInitializer, EventLoopGroup}
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.codec.LengthFieldBasedFrameDecoder
import io.netty.handler.codec.bytes.{ByteArrayDecoder, ByteArrayEncoder}

import org.apache.spark.SparkConf

class BackendChannel(logger: Logger, terminate: () => Unit) {

  private[this] var channelFuture: ChannelFuture = null
  private[this] var bootstrap: ServerBootstrap = null
  private[this] var bossGroup: EventLoopGroup = null
  private[this] var inetAddress: InetSocketAddress = null
  private[this] var hostContext: String = null

  def setHostContext(hostContextParam: String) {
    hostContext = hostContextParam
  }

  def init(remote: Boolean): Int = {
    val conf = new SparkConf()
    logger.log(conf.toDebugString)
    if (remote) {
      val anyIpAddress = Array[Byte](0, 0, 0, 0)
      val anyInetAddress = InetAddress.getByAddress(anyIpAddress)

      inetAddress = new InetSocketAddress(anyInetAddress, conf.getInt("spark.lyr.backend.port", 0))
    }
    else {
      inetAddress = new InetSocketAddress(InetAddress.getLoopbackAddress(), conf.getInt("spark.lyr.backend.port", 0))
    }

    bossGroup = new NioEventLoopGroup(conf.getInt("spark.lyr.backend.threads", 10))
    val workerGroup = bossGroup
    val handler = new BackendHandler(this, logger, hostContext)

    bootstrap = new ServerBootstrap()
      .group(bossGroup, workerGroup)
      .channel(classOf[NioServerSocketChannel])

    bootstrap.childHandler(new ChannelInitializer[SocketChannel]() {
      def initChannel(ch: SocketChannel): Unit = {
        ch.pipeline()
          .addLast("encoder", new ByteArrayEncoder())
          .addLast("frameDecoder",
            new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4))
          .addLast("decoder", new ByteArrayDecoder())
          .addLast("handler", handler)
      }
    })

    channelFuture = bootstrap.bind(inetAddress)
    channelFuture.syncUninterruptibly()
    channelFuture.channel().localAddress().asInstanceOf[InetSocketAddress].getPort()
  }

  def run(): Unit = {
    channelFuture.channel.closeFuture().syncUninterruptibly()
  }

  def close(): Unit = {
    terminate()

    if (channelFuture != null) {
      // close is a local operation and should finish within milliseconds; timeout just to be safe
      channelFuture.channel().close().awaitUninterruptibly(10, TimeUnit.SECONDS)
      channelFuture = null
    }
    if (bootstrap != null && bootstrap.group() != null) {
      bootstrap.group().shutdownGracefully()
    }
    if (bootstrap != null && bootstrap.childGroup() != null) {
      bootstrap.childGroup().shutdownGracefully()
    }
    bootstrap = null
  }

}
