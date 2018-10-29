/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.network.shuffle;

import com.codahale.metrics.*;
import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.*;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.server.OneForOneStreamManager;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.server.StreamManager;
import org.apache.spark.network.shuffle.ExternalShuffleBlockResolver.AppExecId;
import org.apache.spark.network.shuffle.protocol.*;
import org.apache.spark.network.util.TransportConf;
import org.apache.spark.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.apache.spark.network.util.NettyUtils.getRemoteAddress;

/**
 * RPC Handler for a server which can serve shuffle blocks from outside of an Executor process.
 *
 * Handles registering executors and opening shuffle blocks from them. Shuffle blocks are registered
 * with the "one-for-one" strategy, meaning each Transport-layer Chunk is equivalent to one Spark-
 * level shuffle block.
 */
public class ExternalShuffleBlockHandler extends RpcHandler {
  private static final Logger logger = LoggerFactory.getLogger(ExternalShuffleBlockHandler.class);

  @VisibleForTesting
  final ExternalShuffleBlockResolver blockManager;
  private final OneForOneStreamManager streamManager;
  private final ShuffleMetrics metrics;

  public ExternalShuffleBlockHandler(TransportConf conf, File registeredExecutorFile)
    throws IOException {
    this(new OneForOneStreamManager(),
      new ExternalShuffleBlockResolver(conf, registeredExecutorFile));
  }

  /** Enables mocking out the StreamManager and BlockManager. */
  @VisibleForTesting
  public ExternalShuffleBlockHandler(
      OneForOneStreamManager streamManager,
      ExternalShuffleBlockResolver blockManager) {
    this.metrics = new ShuffleMetrics();
    this.streamManager = streamManager;
    this.blockManager = blockManager;
  }

  @Override
  public void receive(TransportClient client, ByteBuffer message, RpcResponseCallback callback) {
    BlockTransferMessage msgObj = BlockTransferMessage.Decoder.fromByteBuffer(message);
    if (msgObj instanceof UploadBlockData) {
      UploadBlockData msg = (UploadBlockData) msgObj;
      handleUploadBlockData(message, msg, client, callback);

    } else if (msgObj instanceof  UploadBlockIndex ) {
      UploadBlockIndex msg = (UploadBlockIndex) msgObj;
      handleUploadBlockIndex(message, msg, client, callback);
    } else {
      handleMessage(msgObj, client, callback);
    }
  }

  protected void handleUploadBlockData(
          ByteBuffer body,
          UploadBlockData msg,
          TransportClient client,
          RpcResponseCallback callback) {

      RandomAccessFile raf = null;
      try{
          checkAuth(client, msg.appId);
          ExecutorShuffleInfo execInfo = blockManager.executors.get(new AppExecId(msg.appId, msg.execId));
          File outputFile;
          File outputDataFile = ExternalShuffleUtils.getFile(execInfo, msg.blockId + ".data");
          //todo to be fixed: change tmpPath by uuid
          String  tmpPath = outputDataFile.getAbsolutePath() + ".shufflewrite";
          if (msg.flag == MessageEnum.SHUFFLE_WRITE) {
              outputFile = new File(tmpPath);
          } else {
              outputFile =  ExternalShuffleUtils.getFile(execInfo, msg.blockId + "_" + msg.flag);
          }
          long statTime = System.currentTimeMillis();
          raf = new RandomAccessFile(outputFile, "rw");
          //todo to be fixed
          if (!body.hasArray()) {
              System.out.format("!body.hasArray() \n");
              ByteBuf bodybuf = Unpooled.wrappedBuffer(body);
              int length = bodybuf.readableBytes();
              byte[] array = new byte[length];
              bodybuf.getBytes(bodybuf.readerIndex(), array);

              raf.seek(msg.offset);
              raf.write(array, msg.encodedLength() + 1, msg.length);
          } else {
              System.out.format("body.hasArray() \n");
              raf.seek(msg.offset);
              raf.write(body.array(), msg.encodedLength() + 1, msg.length);
          }
          logger.info("handleUploadBlockData thread-id:%s appid:%s execId:%s blockId:%s offset:%s length:%s check:%s outputDataPath:%s\n",
                  Thread.currentThread().getId(),
                  msg.appId,
                  msg.execId,
                  msg.blockId,
                  msg.offset,
                  msg.length,
                  msg.length == (body.capacity() - msg.encodedLength() - 1),
                  outputDataFile.getAbsolutePath()
          );
          logger.info("insert data used time--------------:%s \n", System.currentTimeMillis() - statTime);
          callback.onSuccess(ByteBuffer.wrap(new byte[0]));

      } catch (Exception e) {
          e.printStackTrace();
          callback.onFailure(e);
      } finally {
          try {
              if(raf != null) raf.close();
          } catch (Exception e) {
              e.printStackTrace();
              logger.error(e.toString());
          }

      }

  }

  protected void handleUploadBlockIndex(
          ByteBuffer body,
          UploadBlockIndex msg,
          TransportClient client,
          RpcResponseCallback callback) {
    try {
        checkAuth(client, msg.appId);
        ExecutorShuffleInfo execInfo = blockManager.executors.get(new AppExecId(msg.appId, msg.execId));
        String appExecIdBlockID = ExternalShuffleUtils.getAppExecIdBlockID(msg);
        ByteBuf bodybuf = Unpooled.wrappedBuffer(body);
        bodybuf.readerIndex(msg.encodedLength() + 1);
        //todo to be fixed
        int numPartitions = msg.length / 8;
        long[] lengths = new long[numPartitions];

        for (int i = 0; i < numPartitions; i++) {
            lengths[i] = bodybuf.readLong();
        }
        File outputDataFile = ExternalShuffleUtils.getFile(execInfo, msg.blockId + ".data");
        File spillFile;
        if (msg.flag == MessageEnum.SHUFFLE_WRITE) {
            String tmpPath = outputDataFile.getAbsolutePath() + ".shufflewrite";
            spillFile = new File(tmpPath);
        } else {
            spillFile = ExternalShuffleUtils.getFile(execInfo, msg.blockId + "_" + msg.flag);
        }
        SpillInfo spillInfo = new SpillInfo(numPartitions, spillFile, lengths);

        blockManager.updateShuffleindexs(appExecIdBlockID, spillInfo, msg.flag);

        logger.info("handleUploadBlockIndex thread-id:%s appid:%s execId:%s blockId:%s length:%s spillFilePath:%s\n",
                Thread.currentThread().getId(),
                msg.appId,
                msg.execId,
                msg.blockId,
                msg.length,
                spillFile.getAbsolutePath()
        );
        // merge spill
        if (msg.flag == MessageEnum.SHUFFLE_WRITE) {
            File outputIndexFile = ExternalShuffleUtils.getFile(execInfo, msg.blockId + ".index");
            Map<Integer, SpillInfo> map = blockManager.shuffleindexs.get(appExecIdBlockID);
            int numFiles = map.size();
            SpillInfo[] spillInfos = new SpillInfo[numFiles];

            for (Map.Entry<Integer, SpillInfo> e : map.entrySet()) {
                spillInfos[e.getKey()] = e.getValue();
            }
            logger.info("--------------numFiles--------------:%s \n", numFiles);

            File tmp = Utils.tempFileWith(outputDataFile);

            long statTime = System.currentTimeMillis();

            long [] partitionLengths = ExternalShuffleUtils.mergeSpills(spillInfos, tmp, numPartitions);

            logger.info("merge data used time--------------:%s \n", System.currentTimeMillis() - statTime);
            statTime = System.currentTimeMillis();

            blockManager.writeIndexFileAndCommit(outputIndexFile, outputDataFile, tmp, partitionLengths);
            logger.info("create index used time--------------:%s\n\"index--------------partitionLengths:%s\\", System.currentTimeMillis() - statTime, Arrays.toString(partitionLengths));
            ByteArrayOutputStream buf = blockManager.getByteArrayOutputStream(lengths);

            callback.onSuccess(ByteBuffer.wrap(buf.toByteArray()));
        } else {

            callback.onSuccess(ByteBuffer.wrap(new byte[0]));
        }
    } catch (Exception e) {
        e.printStackTrace();
        callback.onFailure(e);

    }

  }

  protected void handleMessage(
      BlockTransferMessage msgObj,
      TransportClient client,
      RpcResponseCallback callback) {
    if (msgObj instanceof OpenBlocks) {
      final Timer.Context responseDelayContext = metrics.openBlockRequestLatencyMillis.time();
      try {
        System.out.format("OpenBlocks thread-id:%s\n", Thread.currentThread().getId());
        OpenBlocks msg = (OpenBlocks) msgObj;
        checkAuth(client, msg.appId);
        long streamId = streamManager.registerStream(client.getClientId(),
          new ManagedBufferIterator(msg.appId, msg.execId, msg.blockIds));
        if (logger.isTraceEnabled()) {
          logger.trace("Registered streamId {} with {} buffers for client {} from host {}",
                       streamId,
                       msg.blockIds.length,
                       client.getClientId(),
                       getRemoteAddress(client.getChannel()));
        }
        callback.onSuccess(new StreamHandle(streamId, msg.blockIds.length).toByteBuffer());
      } finally {
        responseDelayContext.stop();
      }

    } else if (msgObj instanceof RegisterExecutor) {
      final Timer.Context responseDelayContext =
        metrics.registerExecutorRequestLatencyMillis.time();
      try {
        System.out.format("Reister thread-id:%s\n", Thread.currentThread().getId());
        RegisterExecutor msg = (RegisterExecutor) msgObj;
        checkAuth(client, msg.appId);
        blockManager.registerExecutor(msg.appId, msg.execId, msg.executorInfo);
        callback.onSuccess(ByteBuffer.wrap(new byte[0]));
      } finally {
        responseDelayContext.stop();
      }

    } else {
      throw new UnsupportedOperationException("Unexpected message: " + msgObj);
    }
  }

  public MetricSet getAllMetrics() {
    return metrics;
  }
  public class MessageEnum {

    public static final int SHUFFLE_WRITE = 0;

  }

  @Override
  public StreamManager getStreamManager() {
    return streamManager;
  }

  /**
   * Removes an application (once it has been terminated), and optionally will clean up any
   * local directories associated with the executors of that application in a separate thread.
   */
  public void applicationRemoved(String appId, boolean cleanupLocalDirs) {
    blockManager.applicationRemoved(appId, cleanupLocalDirs);
  }

  /**
   * Register an (application, executor) with the given shuffle info.
   *
   * The "re-" is meant to highlight the intended use of this method -- when this service is
   * restarted, this is used to restore the state of executors from before the restart.  Normal
   * registration will happen via a message handled in receive()
   *
   * @param appExecId
   * @param executorInfo
   */
  public void reregisterExecutor(AppExecId appExecId, ExecutorShuffleInfo executorInfo) {
    blockManager.registerExecutor(appExecId.appId, appExecId.execId, executorInfo);
  }

  public void close() {
    blockManager.close();
  }

  private void checkAuth(TransportClient client, String appId) {
    if (client.getClientId() != null && !client.getClientId().equals(appId)) {
      throw new SecurityException(String.format(
        "Client for %s not authorized for application %s.", client.getClientId(), appId));
    }
  }

  /**
   * A simple class to wrap all shuffle service wrapper metrics
   */
  private class ShuffleMetrics implements MetricSet {
    private final Map<String, Metric> allMetrics;
    // Time latency for open block request in ms
    private final Timer openBlockRequestLatencyMillis = new Timer();
    // Time latency for executor registration latency in ms
    private final Timer registerExecutorRequestLatencyMillis = new Timer();
    // Block transfer rate in byte per second
    private final Meter blockTransferRateBytes = new Meter();

    private ShuffleMetrics() {
      allMetrics = new HashMap<>();
      allMetrics.put("openBlockRequestLatencyMillis", openBlockRequestLatencyMillis);
      allMetrics.put("registerExecutorRequestLatencyMillis", registerExecutorRequestLatencyMillis);
      allMetrics.put("blockTransferRateBytes", blockTransferRateBytes);
      allMetrics.put("registeredExecutorsSize",
                     (Gauge<Integer>) () -> blockManager.getRegisteredExecutorsSize());
    }

    @Override
    public Map<String, Metric> getMetrics() {
      return allMetrics;
    }
  }

  private class ManagedBufferIterator implements Iterator<ManagedBuffer> {

    private int index = 0;
    private final String appId;
    private final String execId;
    private final int shuffleId;
    // An array containing mapId and reduceId pairs.
    private final int[] mapIdAndReduceIds;

    ManagedBufferIterator(String appId, String execId, String[] blockIds) {
      this.appId = appId;
      this.execId = execId;
      String[] blockId0Parts = blockIds[0].split("_");
      if (blockId0Parts.length != 4 || !blockId0Parts[0].equals("shuffle")) {
        throw new IllegalArgumentException("Unexpected shuffle block id format: " + blockIds[0]);
      }
      this.shuffleId = Integer.parseInt(blockId0Parts[1]);
      mapIdAndReduceIds = new int[2 * blockIds.length];
      for (int i = 0; i < blockIds.length; i++) {
        String[] blockIdParts = blockIds[i].split("_");
        if (blockIdParts.length != 4 || !blockIdParts[0].equals("shuffle")) {
          throw new IllegalArgumentException("Unexpected shuffle block id format: " + blockIds[i]);
        }
        if (Integer.parseInt(blockIdParts[1]) != shuffleId) {
          throw new IllegalArgumentException("Expected shuffleId=" + shuffleId +
            ", got:" + blockIds[i]);
        }
        mapIdAndReduceIds[2 * i] = Integer.parseInt(blockIdParts[2]);
        mapIdAndReduceIds[2 * i + 1] = Integer.parseInt(blockIdParts[3]);
      }
    }

    @Override
    public boolean hasNext() {
      return index < mapIdAndReduceIds.length;
    }

    @Override
    public ManagedBuffer next() {
      final ManagedBuffer block = blockManager.getBlockData(appId, execId, shuffleId,
        mapIdAndReduceIds[index], mapIdAndReduceIds[index + 1]);
      index += 2;
      metrics.blockTransferRateBytes.mark(block != null ? block.size() : 0);
      return block;
    }
  }

}
