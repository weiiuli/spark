package org.apache.spark.network.shuffle;


import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import org.apache.commons.io.output.CloseShieldOutputStream;
import org.apache.commons.io.output.CountingOutputStream;
import org.apache.spark.SparkConf;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.internal.config.package$;
import org.apache.spark.io.CompressionCodec;
import org.apache.spark.io.CompressionCodec$;
import org.apache.spark.io.NioBufferedFileInputStream;
import org.apache.spark.network.shuffle.protocol.ExecutorShuffleInfo;
import org.apache.spark.network.shuffle.protocol.UploadBlockIndex;
import org.apache.spark.network.util.JavaUtils;
import org.apache.spark.network.util.LimitedInputStream;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.serializer.SerializerManager;
import org.apache.spark.shuffle.sort.UnsafeShuffleWriter;
import org.apache.spark.storage.TempShuffleBlockId;
import org.apache.spark.storage.TimeTrackingOutputStream;
import org.apache.spark.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import javax.annotation.Nullable;
import java.io.*;
import java.nio.channels.FileChannel;
import java.util.UUID;

/**
 * @author: xiuli wei JD.com
 * @commented by: zhen fan JD.com
 * @Date: 2018/10/23
 */

public class ExternalShuffleUtils {

  private static final Logger logger = LoggerFactory.getLogger(ExternalShuffleUtils.class);

  public static SparkConf sparkConf = new SparkConf();
  public static boolean transferToEnabled;
  public static int inputBufferSizeInBytes;
  public static int outputBufferSizeInBytes;
  public static SerializerManager serializerManager;
  static {
    Utils.loadDefaultSparkProperties(sparkConf, null);
    inputBufferSizeInBytes = (int) (long) sparkConf.get(package$.MODULE$.SHUFFLE_FILE_BUFFER_SIZE()) * 1024;
    outputBufferSizeInBytes = (int) (long) sparkConf.get(package$.MODULE$.SHUFFLE_UNSAFE_FILE_OUTPUT_BUFFER_SIZE()) * 1024;
    transferToEnabled = sparkConf.getBoolean("spark.file.transferTo", true);
    serializerManager = new SerializerManager(new KryoSerializer(sparkConf), sparkConf);
    logger.debug("spark.shuffle.file.buffer {}",inputBufferSizeInBytes);
    logger.debug("spark.shuffle.unsafe.file.output.buffer {}",outputBufferSizeInBytes);
  }

  /**
   * Merge zero or more spill files together, choosing the fastest merging strategy based on the
   * number of spills and the IO compression codec.
   *
   * @return the partition lengths in the merged file.
   */
  public static long[] mergeSpills (SpillInfo[] spills, File outputFile, int numPartitions) throws IOException {
    final boolean compressionEnabled = sparkConf.getBoolean("spark.shuffle.compress", true);
    final CompressionCodec compressionCodec = CompressionCodec$.MODULE$.createCodec(sparkConf);
    final boolean fastMergeEnabled =
            sparkConf.getBoolean("spark.shuffle.unsafe.fastMergeEnabled", true);

    logger.debug("spark.shuffle.compress {}", compressionEnabled);
    logger.debug("spark.shuffle.unsafe.fastMergeEnable {}", fastMergeEnabled);

    final boolean fastMergeIsSupported = !compressionEnabled ||
            CompressionCodec$.MODULE$.supportsConcatenationOfSerializedStreams(compressionCodec);
    final boolean encryptionEnabled = false;
    try {
      if (spills.length == 0) {
        new FileOutputStream(outputFile).close(); // Create an empty file
        logger.error("No spills to  merge");
        return new long[numPartitions];
      } else if (spills.length == 1) {
        // Here, we don't need to perform any metrics updates because the bytes written to this
        // output file would have already been counted as shuffle bytes written.
        Files.move(spills[0].file, outputFile);
        logger.info("--------------NOSPILL--------------");

        return spills[0].partitionLengths;
      } else {
        final long[] partitionLengths;
        // There are multiple spills to merge, so none of these spill files' lengths were counted
        // towards our shuffle write count or shuffle write time. If we use the slow merge path,
        // then the final output file's size won't necessarily be equal to the sum of the spill
        // files' sizes. To guard against this case, we look at the output file's actual size when
        // computing shuffle bytes written.
        //
        // We allow the individual merge methods to report their own IO times since different merge
        // strategies use different IO techniques.  We count IO during merge towards the shuffle
        // shuffle write time, which appears to be consistent with the "not bypassing merge-sort"
        // branch in ExternalSorter.
        if (fastMergeEnabled && fastMergeIsSupported) {
          // Compression is disabled or we are using an IO compression codec that supports
          // decompression of concatenated compressed streams, so we can perform a fast spill merge
          // that doesn't need to interpret the spilled bytes.
          if (transferToEnabled && !encryptionEnabled) {
            logger.info("Using transferTo-based fast merge");
            partitionLengths = mergeSpillsWithTransferTo(spills, outputFile, numPartitions);
          } else {
            logger.info("Using fileStream-based fast merge");
            partitionLengths = mergeSpillsWithFileStream(spills, outputFile, null, numPartitions);
          }
        } else {
          logger.info("Using slow merge");
          partitionLengths = mergeSpillsWithFileStream(spills, outputFile, compressionCodec,numPartitions);
        }
        logger.info("--------------SPILL--------------");
        return partitionLengths;
      }

    } catch (IOException e) {
      if (outputFile.exists() && !outputFile.delete()) {
        logger.error("Unable to delete output file {}", outputFile.getPath());
      }
      throw e;
    }
  }

  /**
   * Merges spill files by using NIO's transferTo to concatenate spill partitions' bytes.
   * This is only safe when the IO compression codec and serializer support concatenation of
   * serialized streams.
   *
   * @return the partition lengths in the merged file.
   */
  public static long[] mergeSpillsWithTransferTo (SpillInfo[] spills, File outputFile, int numPartitions) throws IOException {
    assert (spills.length >= 2);
    final long[] partitionLengths = new long[numPartitions];
    final FileChannel[] spillInputChannels = new FileChannel[spills.length];
    final long[] spillInputChannelPositions = new long[spills.length];
    FileChannel mergedFileOutputChannel = null;

    boolean threwException = true;
    try {
      for (int i = 0; i < spills.length; i++) {
        spillInputChannels[i] = new FileInputStream(spills[i].file).getChannel();
      }
      // This file needs to opened in append mode in order to work around a Linux kernel bug that
      // affects transferTo; see SPARK-3948 for more details.
      mergedFileOutputChannel = new FileOutputStream(outputFile, true).getChannel();

      long bytesWrittenToMergedFile = 0;
      for (int partition = 0; partition < numPartitions; partition++) {
        for (int i = 0; i < spills.length; i++) {
          final long partitionLengthInSpill = spills[i].partitionLengths[partition];
          final FileChannel spillInputChannel = spillInputChannels[i];
          Utils.copyFileStreamNIO(
                  spillInputChannel,
                  mergedFileOutputChannel,
                  spillInputChannelPositions[i],
                  partitionLengthInSpill);
          spillInputChannelPositions[i] += partitionLengthInSpill;
          bytesWrittenToMergedFile += partitionLengthInSpill;
          partitionLengths[partition] += partitionLengthInSpill;
        }
      }
      // Check the position after transferTo loop to see if it is in the right position and raise an
      // exception if it is incorrect. The position will not be increased to the expected length
      // after calling transferTo in kernel version 2.6.32. This issue is described at
      // https://bugs.openjdk.java.net/browse/JDK-7052359 and SPARK-3948.
      if (mergedFileOutputChannel.position() != bytesWrittenToMergedFile) {
        throw new IOException(
                "Current position " + mergedFileOutputChannel.position() + " does not equal expected " +
                        "position " + bytesWrittenToMergedFile + " after transferTo. Please check your kernel" +
                        " version to see if it is 2.6.32, as there is a kernel bug which will lead to " +
                        "unexpected behavior when using transferTo. You can set spark.file.transferTo=false " +
                        "to disable this NIO feature."
        );
      }
      threwException = false;
    } finally {
      // To avoid masking exceptions that caused us to prematurely enter the finally block, only
      // throw exceptions during cleanup if threwException == false.
      for (int i = 0; i < spills.length; i++) {
        assert (spillInputChannelPositions[i] == spills[i].file.length());
        Closeables.close(spillInputChannels[i], threwException);
      }
      Closeables.close(mergedFileOutputChannel, threwException);
    }
    return partitionLengths;
  }

  /**
   * Merges spill files using Java FileStreams. This code path is typically slower than
   * the NIO-based merge, {@link UnsafeShuffleWriter#mergeSpillsWithTransferTo(org.apache.spark.shuffle.sort.SpillInfo[],
   * File)}, and it's mostly used in cases where the IO compression codec does not support
   * concatenation of compressed data, when encryption is enabled, or when users have
   * explicitly disabled use of {@code transferTo} in order to work around kernel bugs.
   * This code path might also be faster in cases where individual partition size in a spill
   * is small and UnsafeShuffleWriter#mergeSpillsWithTransferTo method performs many small
   * disk ios which is inefficient. In those case, Using large buffers for input and output
   * files helps reducing the number of disk ios, making the file merging faster.
   *
   * @param spills the spills to merge.
   * @param outputFile the file to write the merged data to.
   * @param compressionCodec the IO compression codec, or null if shuffle compression is disabled.
   * @return the partition lengths in the merged file.
   */
  public static long[] mergeSpillsWithFileStream(
          SpillInfo[] spills,
          File outputFile,
          @Nullable CompressionCodec compressionCodec,
          int numPartitions
  ) throws IOException {
    assert (spills.length >= 2);
    ShuffleWriteMetrics writeMetrics = new ShuffleWriteMetrics();
    final long[] partitionLengths = new long[numPartitions];
    final InputStream[] spillInputStreams = new InputStream[spills.length];

    final OutputStream bos = new BufferedOutputStream(
            new FileOutputStream(outputFile),
            outputBufferSizeInBytes);
    // Use a counting output stream to avoid having to close the underlying file and ask
    // the file system for its size after each partition is written.
    final CountingOutputStream mergedFileOutputStream = new CountingOutputStream(bos);

    boolean threwException = true;
    try {
      for (int i = 0; i < spills.length; i++) {
        spillInputStreams[i] = new NioBufferedFileInputStream(
                spills[i].file,
                inputBufferSizeInBytes);
      }
      for (int partition = 0; partition < numPartitions; partition++) {
        final long initialFileLength = mergedFileOutputStream.getByteCount();
        // Shield the underlying output stream from close() and flush() calls, so that we can close
        // the higher level streams to make sure all data is really flushed and internal state is
        // cleaned.
        OutputStream partitionOutput = new CloseAndFlushShieldOutputStream(
                new TimeTrackingOutputStream(writeMetrics, mergedFileOutputStream));
        partitionOutput = serializerManager.wrapForEncryption(partitionOutput);
        if (compressionCodec != null) {
          partitionOutput = compressionCodec.compressedOutputStream(partitionOutput);
        }
        for (int i = 0; i < spills.length; i++) {
          final long partitionLengthInSpill = spills[i].partitionLengths[partition];
          if (partitionLengthInSpill > 0) {
            InputStream partitionInputStream = new LimitedInputStream(spillInputStreams[i],
                    partitionLengthInSpill, false);
            try {
              partitionInputStream = serializerManager.wrapForEncryption(
                      partitionInputStream);
              if (compressionCodec != null) {
                partitionInputStream = compressionCodec.compressedInputStream(partitionInputStream);
              }
              ByteStreams.copy(partitionInputStream, partitionOutput);
            } finally {
              partitionInputStream.close();
            }
          }
        }
        partitionOutput.flush();
        partitionOutput.close();
        partitionLengths[partition] = (mergedFileOutputStream.getByteCount() - initialFileLength);
      }
      threwException = false;
    } finally {
      // To avoid masking exceptions that caused us to prematurely enter the finally block, only
      // throw exceptions during cleanup if threwException == false.
      for (InputStream stream : spillInputStreams) {
        Closeables.close(stream, threwException);
      }
      Closeables.close(mergedFileOutputStream, threwException);
    }
    return partitionLengths;
  }

  public static class CloseAndFlushShieldOutputStream extends CloseShieldOutputStream {

    CloseAndFlushShieldOutputStream(OutputStream outputStream) {
      super(outputStream);
    }

    @Override
    public void flush() {
      // do nothing
    }
  }
  /**
   * Produces a unique block id and File suitable for storing shuffled intermediate results.
   */
  public static Tuple2<TempShuffleBlockId, File> createTempShuffleBlock(ExecutorShuffleInfo execInfo, String filename) {
    TempShuffleBlockId blockId = new TempShuffleBlockId(UUID.randomUUID());
    while (getFile(execInfo, filename).exists()) {
      blockId = new TempShuffleBlockId(UUID.randomUUID());
    }
    return new Tuple2(blockId, getFile(execInfo, filename));
  }
  public  static File getFile(ExecutorShuffleInfo execInfo, String filename) {
    int hash = JavaUtils.nonNegativeHash(filename);
    int dirId = hash % execInfo.localDirs.length;
    int subDirId = (hash / execInfo.localDirs.length) % execInfo.subDirsPerLocalDir;
    // Create the subdirectory if it doesn't already exist
    File newDir = new File(execInfo.localDirs[dirId], Integer.toString(subDirId & 0xff | 0x100, 16).substring(1));
//    if (!newDir.exists() && !newDir.mkdir()) {
//      logger.error("Failed to create local dir in $newDir");
//    }
    return new File(newDir, filename);
  }

  public static String getAppExecIdBlockID(UploadBlockIndex msg) {

    return msg.appId + "_" + msg.execId + "_" + msg.blockId;
  }
}
