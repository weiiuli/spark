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

package org.apache.spark.network.shuffle

import java.io.{File, IOException}
import java.util.UUID

import org.apache.spark.SparkConf
import org.apache.spark.executor.ExecutorExitCode
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

/**
  * Creates and maintains the logical mapping between logical blocks and physical on-disk
  * locations. One block is mapped to one file with a name given by its BlockId.
  *
  * Block files are hashed among the directories listed in spark.local.dir (or in
  * SPARK_LOCAL_DIRS, if it's set).
  */
private[spark] class ExternalShuffleDiskBlockManager() extends Logging {

  private[spark] val conf = {
    var sparkConf = new SparkConf
    Utils.loadDefaultSparkProperties(sparkConf, null)
    sparkConf
  }
  private[spark] val subDirsPerLocalDir: Int = conf.getInt("spark.diskStore.subDirectories", 64)

  private val MAX_DIR_CREATION_ATTEMPTS: Int = 10
  /* Create one local directory for each path mentioned in spark.local.dir; then, inside this
   * directory, create multiple subdirectories that we will hash files into, in order to avoid
   * having really large inodes at the top level. */
  private[spark] val localDirs: Array[String] = createLocalDirs(conf)
  if (localDirs.isEmpty) {
    logError("Failed to create any local dir.")
    System.exit(ExecutorExitCode.DISK_STORE_FAILED_TO_CREATE_DIR)
  }
  /**
    * Create local directories for storing block data. These directories are
    * located inside configured local directories and won't
    * be deleted on JVM exit when using the external shuffle service.
    */
  private def createLocalDirs(conf: SparkConf): Array[String] = {
    Utils.getConfiguredLocalDirs(conf).flatMap { rootDir =>
      try {
        val localDir = createDirectory(rootDir, "blockmgr")
        logInfo(s"Created local directory at $localDir")
        Some(localDir.toString)
      } catch {
        case e: IOException =>
          logError(s"Failed to create local dir in $rootDir. Ignoring this directory.", e)
          None
      }
    }
  }

  /**
    * Create a directory inside the given parent directory and children  directorys by subDirsPerLocalDir
    * which will be used in shuffling data by ESS, while this only return the parent directory .
    * The directory is guaranteed to be  newly created, and is not marked for automatic deletion.
    */
  private def createDirectory(root: String, namePrefix: String = "spark"): File = {
    var attempts = 0
    val maxAttempts = MAX_DIR_CREATION_ATTEMPTS
    var dir: File = null
    while (dir == null) {
      attempts += 1
      if (attempts > maxAttempts) {
        throw new IOException("Failed to create a temp directory (under " + root + ") after " +
          maxAttempts + " attempts!")
      }
      try {
        dir = new File(root, namePrefix + "-" + UUID.randomUUID.toString)
        if (dir.exists() || !dir.mkdirs()) {
          dir = null
        } else {
          // Here create subDirs  directory inside dir
          var sudiraAtempts = 0
          var sudir: File = null
          for (su <- 0 to subDirsPerLocalDir) {
            sudiraAtempts = 0
            sudir = null
            while (sudir == null) {
              sudiraAtempts += 1
              if (sudiraAtempts > maxAttempts) {
                throw new IOException("Failed to create a temp directory (under " + su.toString + ") after " +
                  maxAttempts + " attempts!")
              }
              try {
                sudir = new File(dir, Integer.toString(su & 0xff | 0x100, 16).substring(1))
                if (sudir.exists() || !sudir.mkdirs()) {
                  sudir = null
                }
              }
              catch {
                case e: SecurityException => sudir = null;
              }
            }
          }
        }
      } catch {
        case e: SecurityException => dir = null;
      }
    }
    dir.getCanonicalFile
  }
}
