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

package org.apache.spark.network.shuffle.protocol;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;
import org.apache.spark.network.protocol.Encodable;
import org.apache.spark.network.protocol.Encoders;

import java.util.Arrays;

/** Contains all configuration necessary for locating the shuffle files of an executor. */
public class ExecutorShuffleInfo implements Encodable {
  /** The base set of local directories that the executor stores its shuffle files in. */
  public final String[] localDirs;
  /** Number of subdirectories created within each localDir. */
  public final int subDirsPerLocalDir;
  /** Shuffle manager (SortShuffleManager) that the executor is using. */
  public final String shuffleManager;
  /** Shuffle manager (shuffleCompressEnabled) that the executor and ESS is using. */
  public final boolean shuffleCompressEnabled;
  /** Shuffle manager (compressionCodec) that the executor and ESS is using. */
  public final String compressionCodec ;
  /** Shuffle manager (serializer) that the executor and ESS is using. */
  public final String serializer ;
  /** Shuffle manager (shuffleUnsafeFastMergeEnabled) that the executor and ESS is using. */
  public final boolean shuffleUnsafeFastMergeEnabled;
  /** Shuffle manager (fileTransferTo) that the ESS is using. */
  public final boolean fileTransferTo;

  @JsonCreator
  public ExecutorShuffleInfo(
      @JsonProperty("localDirs") String[] localDirs,
      @JsonProperty("subDirsPerLocalDir") int subDirsPerLocalDir,
      @JsonProperty("shuffleManager") String shuffleManager,
      @JsonProperty("shuffleCompressEnabled") boolean shuffleCompressEnabled,
      @JsonProperty("compressionCodec") String compressionCodec,
      @JsonProperty("serializer") String serializer,
      @JsonProperty("shuffleUnsafeFastMergeEnabled") boolean shuffleUnsafeFastMergeEnabled,
      @JsonProperty("fileTransferTo") boolean fileTransferTo) {
    this.localDirs = localDirs;
    this.subDirsPerLocalDir = subDirsPerLocalDir;
    this.shuffleManager = shuffleManager;
    this.shuffleCompressEnabled = shuffleCompressEnabled;
    this.compressionCodec = compressionCodec;
    this.serializer = serializer;
    this.shuffleUnsafeFastMergeEnabled = shuffleUnsafeFastMergeEnabled;
    this.fileTransferTo = fileTransferTo;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(subDirsPerLocalDir, shuffleManager,compressionCodec,serializer) * 41 +Objects.hashCode(shuffleCompressEnabled,shuffleUnsafeFastMergeEnabled,fileTransferTo) + Arrays.hashCode(localDirs);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("localDirs", Arrays.toString(localDirs))
      .add("subDirsPerLocalDir", subDirsPerLocalDir)
      .add("shuffleManager", shuffleManager)
      .add("shuffleCompressEnabled", shuffleCompressEnabled)
      .add("compressionCodec", compressionCodec)
      .add("serializer", serializer)
      .add("shuffleUnsafeFastMergeEnabled", shuffleUnsafeFastMergeEnabled)
      .add("fileTransferTo", fileTransferTo)
      .toString();
  }

  @Override
  public boolean equals(Object other) {
    if (other != null && other instanceof ExecutorShuffleInfo) {
      ExecutorShuffleInfo o = (ExecutorShuffleInfo) other;
      return Arrays.equals(localDirs, o.localDirs)
        && Objects.equal(subDirsPerLocalDir, o.subDirsPerLocalDir)
        && Objects.equal(shuffleManager, o.shuffleManager)
        && Objects.equal(shuffleCompressEnabled, o.shuffleCompressEnabled)
        && Objects.equal(compressionCodec, o.compressionCodec)
        && Objects.equal(serializer, o.serializer)
        && Objects.equal(shuffleUnsafeFastMergeEnabled, o.shuffleUnsafeFastMergeEnabled)
        && Objects.equal(fileTransferTo, o.fileTransferTo);
    }
    return false;
  }

  @Override
  public int encodedLength() {
    return Encoders.StringArrays.encodedLength(localDirs)
        + 4 // int
        + Encoders.Strings.encodedLength(shuffleManager)
        + 1
        + Encoders.Strings.encodedLength(compressionCodec)
        + Encoders.Strings.encodedLength(serializer)
        + 1
        + 1;
  }

  @Override
  public void encode(ByteBuf buf) {
    Encoders.StringArrays.encode(buf, localDirs);
    buf.writeInt(subDirsPerLocalDir);
    Encoders.Strings.encode(buf, shuffleManager);
    buf.writeBoolean(shuffleCompressEnabled);
    Encoders.Strings.encode(buf, compressionCodec);
    Encoders.Strings.encode(buf, serializer);
    buf.writeBoolean(shuffleUnsafeFastMergeEnabled);
    buf.writeBoolean(fileTransferTo);
  }

  public static ExecutorShuffleInfo decode(ByteBuf buf) {
    String[] localDirs = Encoders.StringArrays.decode(buf);
    int subDirsPerLocalDir = buf.readInt();
    String shuffleManager = Encoders.Strings.decode(buf);
    boolean shuffleCompressEnabled = buf.readBoolean();
    String compressionCodec = Encoders.Strings.decode(buf);
    String serializer = Encoders.Strings.decode(buf);
    boolean shuffleUnsafeFastMergeEnabled = buf.readBoolean();
    boolean fileTransferTo = buf.readBoolean();
    return new ExecutorShuffleInfo(localDirs, subDirsPerLocalDir, shuffleManager,shuffleCompressEnabled,compressionCodec,serializer,shuffleUnsafeFastMergeEnabled,fileTransferTo);
  }
}
