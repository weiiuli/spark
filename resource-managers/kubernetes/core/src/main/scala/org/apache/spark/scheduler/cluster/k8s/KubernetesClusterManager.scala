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
package org.apache.spark.scheduler.cluster.k8s

import java.io.File
import java.util.concurrent.TimeUnit

import com.google.common.cache.CacheBuilder
import io.fabric8.kubernetes.client.{Config, KubernetesClient}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.SparkKubernetesClientFactory
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{ExternalClusterManager, SchedulerBackend, TaskScheduler, TaskSchedulerImpl}
import org.apache.spark.util.{SystemClock, ThreadUtils}
import org.apache.spark.{SparkConf, SparkContext}

trait ManagerSpecificHandlers {
  def createKubernetesClient(sparkConf: SparkConf): KubernetesClient
}

private[spark] class KubernetesClusterManager extends ExternalClusterManager with ManagerSpecificHandlers with Logging {

  override def canCreate(masterURL: String): Boolean = masterURL.startsWith("k8s")

  override def createTaskScheduler(sc: SparkContext, masterURL: String): TaskScheduler = {
    new TaskSchedulerImpl(sc)
  }

  class InClusterHandlers extends ManagerSpecificHandlers {
    override def createKubernetesClient(sparkConf: SparkConf): KubernetesClient = {
      logWarning(s"InClusterHandlers----createKubernetesClient--spark.master:: " + KUBERNETES_MASTER_INTERNAL_URL)
      SparkKubernetesClientFactory.createKubernetesClient(
        KUBERNETES_MASTER_INTERNAL_URL,
        Some(sparkConf.get(KUBERNETES_NAMESPACE)),
        KUBERNETES_AUTH_DRIVER_MOUNTED_CONF_PREFIX,
        sparkConf,
        Some(new File(Config.KUBERNETES_SERVICE_ACCOUNT_TOKEN_PATH)),
        Some(new File(Config.KUBERNETES_SERVICE_ACCOUNT_CA_CRT_PATH)))
    }
  }

  class OutClusterHandlers extends ManagerSpecificHandlers {
    override def createKubernetesClient(sparkConf: SparkConf): KubernetesClient = {
      logWarning(s"OutClusterHandlers----createKubernetesClient--spark.master:: " + sparkConf.get("spark.master").replace("k8s://", ""))
      SparkKubernetesClientFactory.createKubernetesClient(
        sparkConf.get("spark.master").replace("k8s://", ""),
        Some(sparkConf.get(KUBERNETES_NAMESPACE)),
        KUBERNETES_AUTH_DRIVER_MOUNTED_CONF_PREFIX,
        sparkConf,
        Some(new File(Config.KUBERNETES_SERVICE_ACCOUNT_TOKEN_PATH)),
        Some(new File(Config.KUBERNETES_SERVICE_ACCOUNT_CA_CRT_PATH)))
    }
  }
  override def createKubernetesClient(sparkConf: SparkConf): KubernetesClient = null

  override def createSchedulerBackend(
                                       sc: SparkContext,
                                       masterURL: String,
                                       scheduler: TaskScheduler): SchedulerBackend = {
    val modeHandler: ManagerSpecificHandlers = {
      new java.io.File(Config.KUBERNETES_SERVICE_ACCOUNT_TOKEN_PATH).exists() match {
        case true => new InClusterHandlers()
        case false => new OutClusterHandlers()
      }
    }
    val kubernetesClient = modeHandler.createKubernetesClient(sc.conf)
    val requestExecutorsService = ThreadUtils.newDaemonCachedThreadPool(
      "kubernetes-executor-requests")

    val subscribersExecutor = ThreadUtils
      .newDaemonThreadPoolScheduledExecutor(
        "kubernetes-executor-snapshots-subscribers", 2)
    val snapshotsStore = new ExecutorPodsSnapshotsStoreImpl(subscribersExecutor)
    val removedExecutorsCache = CacheBuilder.newBuilder()
      .expireAfterWrite(3, TimeUnit.MINUTES)
      .build[java.lang.Long, java.lang.Long]()
    val executorPodsLifecycleEventHandler = new ExecutorPodsLifecycleManager(
      sc.conf,
      new KubernetesExecutorBuilder(),
      kubernetesClient,
      snapshotsStore,
      removedExecutorsCache)

    val executorPodsAllocator = new ExecutorPodsAllocator(
      sc.conf, new KubernetesExecutorBuilder(), kubernetesClient, snapshotsStore, new SystemClock())

    val podsWatchEventSource = new ExecutorPodsWatchSnapshotSource(
      snapshotsStore,
      kubernetesClient)

    val eventsPollingExecutor = ThreadUtils.newDaemonSingleThreadScheduledExecutor(
      "kubernetes-executor-pod-polling-sync")
    val podsPollingEventSource = new ExecutorPodsPollingSnapshotSource(
      sc.conf, kubernetesClient, snapshotsStore, eventsPollingExecutor)

    new KubernetesClusterSchedulerBackend(
      scheduler.asInstanceOf[TaskSchedulerImpl],
      sc.env.rpcEnv,
      kubernetesClient,
      requestExecutorsService,
      snapshotsStore,
      executorPodsAllocator,
      executorPodsLifecycleEventHandler,
      podsWatchEventSource,
      podsPollingEventSource)
  }

  override def initialize(scheduler: TaskScheduler, backend: SchedulerBackend): Unit = {
    scheduler.asInstanceOf[TaskSchedulerImpl].initialize(backend)
  }
}
