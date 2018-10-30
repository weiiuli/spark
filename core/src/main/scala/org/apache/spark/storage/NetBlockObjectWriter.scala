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

package org.apache.spark.storage

import java.io._
import java.nio.ByteBuffer
import java.util.concurrent.TimeUnit

import com.google.common.util.concurrent.SettableFuture
import io.netty.buffer.{ByteBuf, ByteBufOutputStream, PooledByteBufAllocator}
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.network.shuffle.ExternalShuffleClient
import org.apache.spark.serializer.{SerializationStream, SerializerInstance, SerializerManager}
import org.apache.spark.util.Utils

import scala.collection.mutable.ArrayBuffer

/**
 * A class for writing JVM objects directly to a file on disk. This class allows data to be appended
 * to an existing block. For efficiency, it retains the underlying file channel across
 * multiple commits. This channel is kept open until close() is called. In case of faults,
 * callers should instead close with revertPartialWritesAndClose() to atomically revert the
 * uncommitted partial writes.
 *
 * This class does not support concurrent writes. Also, once the writer has been opened it cannot be
 * reopened again.
 */
private[spark] class NetBlockObjectWriter(
    blockManager: BlockManager,
    serializerManager: SerializerManager,
    serializerInstance: SerializerInstance,
    bufferSize: Int,
    syncWrites: Boolean,
    // These write metrics concurrently shared with other active DiskBlockObjectWriters who
    // are themselves performing writes. All updates must be relative.
    writeMetrics: ShuffleWriteMetrics,
    numPartitions: Int,
    flag: Int,
    results: ArrayBuffer[SettableFuture[ByteBuffer]],
    taskUUID: String,
    timeoutMs: Int,
    val blockId: BlockId = null)
  extends OutputStream
  with Logging {

  private var buf: ByteBuf = null
  private var objOut: SerializationStream = null
  private var initialized = false
  private var streamOpen = false
  private var hasBeenClosed = false

  private var paritionLengths: Array[Long] = null
  private var lastPosition: Long = 0

  private var committedPosition = 0
  private var reportedPosition = committedPosition

  /**
   * Keep track of number of records written and also use this to periodically
   * output bytes written since the latter is expensive to do for each record.
   * And we reset it after every commitAndGet called.
   */
  private var numRecordsWritten = 0

  private def initialize(): Unit = {
    paritionLengths = new Array[Long](numPartitions)
  }

  def open(): NetBlockObjectWriter = {
    if (hasBeenClosed) {
      throw new IllegalStateException("Writer already closed. Cannot be reopened.")
    }
    if (!initialized) {
      initialize()
      initialized = true
    }

    buf = PooledByteBufAllocator.DEFAULT.directBuffer(bufferSize * 2)
    val ts = new TimeTrackingOutputStream(writeMetrics, new ByteBufOutputStream(buf))
    val bs = serializerManager.wrapStream(blockId, ts)
    objOut = serializerInstance.serializeStream(bs)
    streamOpen = true
    this
  }

  /**
   * Close and cleanup all resources.
   * Should call after committing or reverting partial writes.
   */
  private def closeResources(): Unit = {
    if (initialized) {
      {
        buf = null
        objOut = null
        initialized = false
        streamOpen = false
        hasBeenClosed = true

        paritionLengths = null
        lastPosition = 0
        committedPosition = 0
        reportedPosition = 0
      }
    }
  }

  /**
   * Commits any remaining partial writes and closes resources.
   */
  override def close() {
    if (initialized) {
      Utils.tryWithSafeFinally {
        flushAndCommit()
      } {
        closeResources()
      }
    }
  }

  /**
   * Flush the partial writes and commit them as a single atomic block.
   * A commit may write additional bytes to frame the atomic block.
   *
   * @return file segment with previous offset and length committed on this call.
   */
  def commitAndGet(): Array[Long] = {
    flushAndCommit()

    if (initialized) {
      if(syncWrites || 0 == flag) {
        val statTime = System.currentTimeMillis
        results.foreach(ret => ret.get(timeoutMs, TimeUnit.MILLISECONDS))
        logInfo(s"Received all responses for UploadBlockData, time: ${System.currentTimeMillis - statTime} ms")
      }

      var buffer: ByteBuf = null
      buffer = PooledByteBufAllocator.DEFAULT.directBuffer(8 * numPartitions)
      for (length <- paritionLengths) {
        buffer.writeLong(length)
      }

      val statTime = System.currentTimeMillis
      val result = blockManager.shuffleClient.asInstanceOf[ExternalShuffleClient].uploadBlockIndex(
        blockManager.shuffleServerId.host,
        blockManager.shuffleServerId.port,
        blockManager.shuffleServerId.executorId,
        blockId.name + "_" + taskUUID,
        flag,
        buffer.readableBytes(),
        numPartitions,
        buffer,
        timeoutMs
      )
      logInfo(s"Received response for UploadBlockIndex, flag: $flag time: ${System.currentTimeMillis - statTime} ms")

      paritionLengths
    } else {
      new Array[Long](numPartitions)
    }
  }

  def flushAndCommit() {
    if (streamOpen) {
      // NOTE: Because Kryo doesn't flush the underlying stream we explicitly flush both the
      //       serializer stream and the lower level stream.
      objOut.flush()
      objOut.close()
      streamOpen = false

      val result = blockManager.shuffleClient.asInstanceOf[ExternalShuffleClient].uploadBlockData(
        blockManager.shuffleServerId.host,
        blockManager.shuffleServerId.port,
        blockManager.shuffleServerId.executorId,
        blockId.name + "_" + taskUUID,
        flag,
        buf.readableBytes(),
        committedPosition,
        buf
      )
      results += result

      committedPosition += buf.readableBytes()
      // In certain compression codecs, more bytes are written after streams are closed
      writeMetrics.incBytesWritten(committedPosition - reportedPosition)
      reportedPosition = committedPosition
      numRecordsWritten = 0
    }
  }

  def recordParition(paritionId: Int) = {
    if (!streamOpen) {
      open()
    }

    objOut.flush()
    val curPosition = buf.readableBytes() + committedPosition

    paritionLengths(paritionId) = curPosition - lastPosition
    lastPosition = curPosition
  }

  /**
   * Writes a key-value pair.
   */
  def write(key: Any, value: Any) {
    if (!streamOpen) {
      open()
    }

    objOut.writeKey(key)
    objOut.writeValue(value)
    recordWritten()
    isFlushBuffer()
  }

  def isFlushBuffer(){
    if (buf.readableBytes() >= bufferSize) {
      flushAndCommit()
    }
  }

  override def write(b: Int): Unit = throw new UnsupportedOperationException()

  override def write(kvBytes: Array[Byte], offs: Int, len: Int): Unit = throw new UnsupportedOperationException()

  /**
   * Notify the writer that a record worth of bytes has been written with OutputStream#write.
   */
  def recordWritten(): Unit = {
    numRecordsWritten += 1
    writeMetrics.incRecordsWritten(1)

    if (numRecordsWritten % 16384 == 0) {
      updateBytesWritten()
    }
  }

  /**
   * Report the number of bytes written in this writer's shuffle write metrics.
   * Note that this is only valid before the underlying streams are closed.
   */
  private def updateBytesWritten() {
    writeMetrics.incBytesWritten(committedPosition - reportedPosition)
    reportedPosition = committedPosition
  }

  // For testing
  private[spark] override def flush() {
    objOut.flush()
  }
}
