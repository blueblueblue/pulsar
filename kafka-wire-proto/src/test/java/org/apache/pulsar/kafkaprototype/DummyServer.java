package org.apache.pulsar.kafkaprototype;

import static org.apache.kafka.common.protocol.ApiKeys.*;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractService;

import java.nio.ByteBuffer;
import java.net.InetSocketAddress;
import java.util.*;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DummyServer extends AbstractService {
    private static final Logger log = LoggerFactory.getLogger(DummyServer.class);

    final EventLoopGroup bossGroup;
    final EventLoopGroup workerGroup;
    final ServerBootstrap bootstrap;
    int port = 0;
    DummyServer() {
        bossGroup = new NioEventLoopGroup();
        workerGroup = new NioEventLoopGroup();
        bootstrap = new ServerBootstrap();
        bootstrap.group(bossGroup, workerGroup)
            .channel(NioServerSocketChannel.class)
            .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline().addLast(new Handler());
                    }
                })
            .option(ChannelOption.SO_BACKLOG, 128)
            .childOption(ChannelOption.SO_KEEPALIVE, true);
    }

    @Override
    public void doStart() {
        ChannelFuture future = bootstrap.bind(0);
        future.addListener((f) -> {
                if (f.isSuccess()) {
                    port = ((InetSocketAddress)future.channel().localAddress()).getPort();
                    notifyStarted();
                } else {
                    notifyFailed(f.cause());
                }
            });
    }

    @Override
    public void doStop() {
        workerGroup.shutdownGracefully();
        bossGroup.shutdownGracefully();
        notifyStopped();
    }

    public int getPort() { return port; }

    class Handler extends ChannelInboundHandlerAdapter {

        ByteBuf sendResponse(RequestHeader header, AbstractResponse response) {
            ByteBuffer serialized = response.serialize(header.apiVersion(), header.toResponseHeader());
            int size = serialized.remaining();
            ByteBuf buf = Unpooled.buffer(4 + size);
            buf.writeInt(size);
            buf.writeBytes(serialized);
            return buf;
        }

        ByteBuf handleApiVersions(RequestHeader header) {
            return sendResponse(header, ApiVersionsResponse.defaultApiVersionsResponse());
        }

        ByteBuf handleMetadata(RequestHeader header, MetadataRequest req) {
            log.info("Metadata request for topics {}", req.topics());

            Node node = new Node(0, "localhost", getPort());
            List<MetadataResponse.TopicMetadata> metadata = new ArrayList<>();
            for (String topic : req.topics()) {
                metadata.add(new MetadataResponse.TopicMetadata(
                                     Errors.NONE, topic, false,
                                     Lists.newArrayList(
                                             new MetadataResponse.PartitionMetadata(Errors.NONE, 0, node,
                                                                                    Lists.newArrayList(node),
                                                                                    Lists.newArrayList(node),
                                                                                    Lists.newArrayList()))));
            }

            MetadataResponse response = new MetadataResponse(
                    Lists.newArrayList(node),
                    "blahblahcluster", 7000, metadata);
            return sendResponse(header, response);
        }

        ByteBuf handleProduce(RequestHeader header, ProduceRequest req) {
            Map<TopicPartition, ProduceResponse.PartitionResponse> resp = new HashMap<>();
            for (Map.Entry<TopicPartition, MemoryRecords> e : req.partitionRecordsOrFail().entrySet()) {
                log.info("Handling produce for partition {} - {}", e.getKey(), e.getValue());
                resp.put(e.getKey(), new ProduceResponse.PartitionResponse(Errors.NONE));
            }
            return sendResponse(header, new ProduceResponse(resp));
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            ByteBuf bb = (ByteBuf)msg;
            int size = bb.readInt();
            ByteBuffer nio = bb.nioBuffer();
            RequestHeader header = RequestHeader.parse(nio);

            log.info("got message {} {}, header {}", msg, size, header);
            switch (header.apiKey()) {
            case API_VERSIONS:
                ctx.writeAndFlush(handleApiVersions(header));
                break;
            case METADATA:
                MetadataRequest mdReq = MetadataRequest.parse(nio, header.apiVersion());
                ctx.writeAndFlush(handleMetadata(header, mdReq));
                break;
            case PRODUCE:
                ProduceRequest pReq = ProduceRequest.parse(nio, header.apiVersion());
                ctx.writeAndFlush(handleProduce(header, pReq));
                break;
            }

            ((ByteBuf) msg).release();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            log.error("got error", cause);
            ctx.close();
        }
    }
}
