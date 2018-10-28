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

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;
import org.apache.spark.network.protocol.Encoders;


public class UploadBlockData extends BlockTransferMessage {
  public final String appId;
  public final String execId;
  public final String blockId;
  public final int flag;
  public final int length;
  public final long offset;

  
  public UploadBlockData(
      String appId,
      String execId,
      String blockId,
      int flag,
      int length,
      long offset) {
    this.appId = appId;
    this.execId = execId;
    this.blockId = blockId;
    this.flag = flag;
    this.length = length;
    this.offset = offset;
  }

  @Override
  protected Type type() { return Type.UPLOAD_BLOCK_DATA; }

  @Override
  public int hashCode() { return Objects.hashCode(appId, execId, blockId, flag, length, offset); }


  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("appId", appId)
      .add("execId", execId)
      .add("blockId", blockId)
      .add("flag", flag)
      .add("length", length)
      .add("offset", offset)
      .toString();
  }

  @Override
  public boolean equals(Object other) {
    if (other != null && other instanceof UploadBlockData) {
      UploadBlockData o = (UploadBlockData) other;
      return Objects.equal(appId, o.appId)
        && Objects.equal(execId, o.execId)
        && Objects.equal(blockId, o.blockId)
        && Objects.equal(flag, o.flag)
        && Objects.equal(offset, o.offset)
        && Objects.equal(length, o.length);
    }
    return false;
  }

  @Override
  public int encodedLength() {
    return Encoders.Strings.encodedLength(appId)
      + Encoders.Strings.encodedLength(execId)
      + Encoders.Strings.encodedLength(blockId)
      + 4 + 4 + 8;
  }

  @Override
  public void encode(ByteBuf buf) {
    Encoders.Strings.encode(buf, appId);
    Encoders.Strings.encode(buf, execId);
    Encoders.Strings.encode(buf, blockId);
    buf.writeInt(flag);
    buf.writeInt(length);
    buf.writeLong(offset);
  }

  public static UploadBlockData decode(ByteBuf buf) {
    String appId = Encoders.Strings.decode(buf);
    String execId = Encoders.Strings.decode(buf);
    String blockId = Encoders.Strings.decode(buf);
    int flag = buf.readInt();
    int length = buf.readInt();
    long offset = buf.readLong();
    return new UploadBlockData(appId, execId, blockId, flag, length, offset);
  }
}
