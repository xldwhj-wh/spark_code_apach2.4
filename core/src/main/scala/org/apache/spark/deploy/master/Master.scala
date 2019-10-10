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

package org.apache.spark.deploy.master

import java.text.SimpleDateFormat
import java.util.{Date, Locale}
import java.util.concurrent.{ScheduledFuture, TimeUnit}

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import scala.util.Random

import org.apache.spark.{SecurityManager, SparkConf, SparkException}
import org.apache.spark.deploy.{ApplicationDescription, DriverDescription,
  ExecutorState, SparkHadoopUtil}
import org.apache.spark.deploy.DeployMessages._
import org.apache.spark.deploy.master.DriverState.DriverState
import org.apache.spark.deploy.master.MasterMessages._
import org.apache.spark.deploy.master.ui.MasterWebUI
import org.apache.spark.deploy.rest.StandaloneRestServer
import org.apache.spark.internal.Logging
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.rpc._
import org.apache.spark.serializer.{JavaSerializer, Serializer}
import org.apache.spark.util.{SparkUncaughtExceptionHandler, ThreadUtils, Utils}

private[deploy] class Master(
    override val rpcEnv: RpcEnv,
    address: RpcAddress,
    webUiPort: Int,
    val securityMgr: SecurityManager,
    val conf: SparkConf)
  extends ThreadSafeRpcEndpoint with Logging with LeaderElectable {

  private val forwardMessageThread =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("master-forward-message-thread")

  private val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)

  // For application IDs
  private def createDateFormat = new SimpleDateFormat("yyyyMMddHHmmss", Locale.US)

  private val WORKER_TIMEOUT_MS = conf.getLong("spark.worker.timeout", 60) * 1000
  private val RETAINED_APPLICATIONS = conf.getInt("spark.deploy.retainedApplications", 200)
  private val RETAINED_DRIVERS = conf.getInt("spark.deploy.retainedDrivers", 200)
  private val REAPER_ITERATIONS = conf.getInt("spark.dead.worker.persistence", 15)
  private val RECOVERY_MODE = conf.get("spark.deploy.recoveryMode", "NONE")
  private val MAX_EXECUTOR_RETRIES = conf.getInt("spark.deploy.maxExecutorRetries", 10)

  val workers = new HashSet[WorkerInfo]
  val idToApp = new HashMap[String, ApplicationInfo]
  private val waitingApps = new ArrayBuffer[ApplicationInfo]
  val apps = new HashSet[ApplicationInfo]

  private val idToWorker = new HashMap[String, WorkerInfo]
  private val addressToWorker = new HashMap[RpcAddress, WorkerInfo]

  private val endpointToApp = new HashMap[RpcEndpointRef, ApplicationInfo]
  private val addressToApp = new HashMap[RpcAddress, ApplicationInfo]
  private val completedApps = new ArrayBuffer[ApplicationInfo]
  private var nextAppNumber = 0

  private val drivers = new HashSet[DriverInfo]
  private val completedDrivers = new ArrayBuffer[DriverInfo]
  // Drivers currently spooled for scheduling
  private val waitingDrivers = new ArrayBuffer[DriverInfo]
  private var nextDriverNumber = 0

  Utils.checkHost(address.host)

  private val masterMetricsSystem = MetricsSystem.createMetricsSystem("master", conf, securityMgr)
  private val applicationMetricsSystem = MetricsSystem.createMetricsSystem("applications", conf,
    securityMgr)
  private val masterSource = new MasterSource(this)

  // After onStart, webUi will be set
  private var webUi: MasterWebUI = null

  private val masterPublicAddress = {
    val envVar = conf.getenv("SPARK_PUBLIC_DNS")
    if (envVar != null) envVar else address.host
  }

  private val masterUrl = address.toSparkURL
  private var masterWebUiUrl: String = _

  private var state = RecoveryState.STANDBY

  private var persistenceEngine: PersistenceEngine = _

  private var leaderElectionAgent: LeaderElectionAgent = _

  private var recoveryCompletionTask: ScheduledFuture[_] = _

  private var checkForWorkerTimeOutTask: ScheduledFuture[_] = _

  // As a temporary workaround before better ways of configuring memory, we allow users to set
  // a flag that will perform round-robin scheduling across the nodes (spreading out each app
  // among all the nodes) instead of trying to consolidate each app onto a small # of nodes.
  private val spreadOutApps = conf.getBoolean("spark.deploy.spreadOut", true)

  // Default maxCores for applications that don't specify it (i.e. pass Int.MaxValue)
  private val defaultCores = conf.getInt("spark.deploy.defaultCores", Int.MaxValue)
  val reverseProxy = conf.getBoolean("spark.ui.reverseProxy", false)
  if (defaultCores < 1) {
    throw new SparkException("spark.deploy.defaultCores must be positive")
  }

  // Alternative application submission gateway that is stable across Spark versions
  private val restServerEnabled = conf.getBoolean("spark.master.rest.enabled", false)
  private var restServer: Option[StandaloneRestServer] = None
  private var restServerBoundPort: Option[Int] = None

  {
    val authKey = SecurityManager.SPARK_AUTH_SECRET_CONF
    require(conf.getOption(authKey).isEmpty || !restServerEnabled,
      s"The RestSubmissionServer does not support authentication via ${authKey}.  Either turn " +
        "off the RestSubmissionServer with spark.master.rest.enabled=false, or do not use " +
        "authentication.")
  }

  override def onStart(): Unit = {
    logInfo("Starting Spark master at " + masterUrl)
    logInfo(s"Running Spark version ${org.apache.spark.SPARK_VERSION}")
    webUi = new MasterWebUI(this, webUiPort)
    webUi.bind()
    masterWebUiUrl = "http://" + masterPublicAddress + ":" + webUi.boundPort
    if (reverseProxy) {
      masterWebUiUrl = conf.get("spark.ui.reverseProxyUrl", masterWebUiUrl)
      webUi.addProxy()
      logInfo(s"Spark Master is acting as a reverse proxy. Master, Workers and " +
       s"Applications UIs are available at $masterWebUiUrl")
    }
    checkForWorkerTimeOutTask = forwardMessageThread.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = Utils.tryLogNonFatalError {
        self.send(CheckForWorkerTimeOut)
      }
    }, 0, WORKER_TIMEOUT_MS, TimeUnit.MILLISECONDS)

    if (restServerEnabled) {
      val port = conf.getInt("spark.master.rest.port", 6066)
      restServer = Some(new StandaloneRestServer(address.host, port, conf, self, masterUrl))
    }
    restServerBoundPort = restServer.map(_.start())

    masterMetricsSystem.registerSource(masterSource)
    masterMetricsSystem.start()
    applicationMetricsSystem.start()
    // Attach the master and app metrics servlet handler to the web ui after the metrics systems are
    // started.
    masterMetricsSystem.getServletHandlers.foreach(webUi.attachHandler)
    applicationMetricsSystem.getServletHandlers.foreach(webUi.attachHandler)

    val serializer = new JavaSerializer(conf)
    val (persistenceEngine_, leaderElectionAgent_) = RECOVERY_MODE match {
      case "ZOOKEEPER" =>
        logInfo("Persisting recovery state to ZooKeeper")
        val zkFactory =
          new ZooKeeperRecoveryModeFactory(conf, serializer)
        (zkFactory.createPersistenceEngine(), zkFactory.createLeaderElectionAgent(this))
      case "FILESYSTEM" =>
        val fsFactory =
          new FileSystemRecoveryModeFactory(conf, serializer)
        (fsFactory.createPersistenceEngine(), fsFactory.createLeaderElectionAgent(this))
      case "CUSTOM" =>
        val clazz = Utils.classForName(conf.get("spark.deploy.recoveryMode.factory"))
        val factory = clazz.getConstructor(classOf[SparkConf], classOf[Serializer])
          .newInstance(conf, serializer)
          .asInstanceOf[StandaloneRecoveryModeFactory]
        (factory.createPersistenceEngine(), factory.createLeaderElectionAgent(this))
      case _ =>
        (new BlackHolePersistenceEngine(), new MonarchyLeaderAgent(this))
    }
    persistenceEngine = persistenceEngine_
    leaderElectionAgent = leaderElectionAgent_
  }

  override def onStop() {
    masterMetricsSystem.report()
    applicationMetricsSystem.report()
    // prevent the CompleteRecovery message sending to restarted master
    if (recoveryCompletionTask != null) {
      recoveryCompletionTask.cancel(true)
    }
    if (checkForWorkerTimeOutTask != null) {
      checkForWorkerTimeOutTask.cancel(true)
    }
    forwardMessageThread.shutdownNow()
    webUi.stop()
    restServer.foreach(_.stop())
    masterMetricsSystem.stop()
    applicationMetricsSystem.stop()
    persistenceEngine.close()
    leaderElectionAgent.stop()
  }

  override def electedLeader() {
    self.send(ElectedLeader)
  }

  override def revokedLeadership() {
    self.send(RevokedLeadership)
  }

  override def receive: PartialFunction[Any, Unit] = {
    case ElectedLeader =>
      val (storedApps, storedDrivers, storedWorkers) = persistenceEngine.readPersistedData(rpcEnv)
      state = if (storedApps.isEmpty && storedDrivers.isEmpty && storedWorkers.isEmpty) {
        RecoveryState.ALIVE
      } else {
        RecoveryState.RECOVERING
      }
      logInfo("I have been elected leader! New state: " + state)
      if (state == RecoveryState.RECOVERING) {
        beginRecovery(storedApps, storedDrivers, storedWorkers)
        recoveryCompletionTask = forwardMessageThread.schedule(new Runnable {
          override def run(): Unit = Utils.tryLogNonFatalError {
            self.send(CompleteRecovery)
          }
        }, WORKER_TIMEOUT_MS, TimeUnit.MILLISECONDS)
      }

    case CompleteRecovery => completeRecovery()

    case RevokedLeadership =>
      logError("Leadership has been revoked -- master shutting down.")
      System.exit(0)

    case RegisterWorker(
      id, workerHost, workerPort, workerRef, cores, memory, workerWebUiUrl, masterAddress) =>
      logInfo("Registering worker %s:%d with %d cores, %s RAM".format(
        workerHost, workerPort, cores, Utils.megabytesToString(memory)))
      if (state == RecoveryState.STANDBY) {
        workerRef.send(MasterInStandby)
      } else if (idToWorker.contains(id)) {
        workerRef.send(RegisterWorkerFailed("Duplicate worker ID"))
      } else {
        val worker = new WorkerInfo(id, workerHost, workerPort, cores, memory,
          workerRef, workerWebUiUrl)
        if (registerWorker(worker)) {
          persistenceEngine.addWorker(worker)
          workerRef.send(RegisteredWorker(self, masterWebUiUrl, masterAddress))
          schedule()
        } else {
          val workerAddress = worker.endpoint.address
          logWarning("Worker registration failed. Attempted to re-register worker at same " +
            "address: " + workerAddress)
          workerRef.send(RegisterWorkerFailed("Attempted to re-register worker at same address: "
            + workerAddress))
        }
      }

    case RegisterApplication(description, driver) =>
      // TODO Prevent repeated registrations from some driver
      // 如果master的状态是standby，也就是当前这个接收消息的master是standby master，不是active master
      // 那么Application的注册请求什么都不做
      if (state == RecoveryState.STANDBY) {
        // ignore, don't send response
      } else {
        logInfo("Registering app " + description.name)
        // 使用ApplicationDescription调用createApplication创建一个ApplicationInfo
        val app = createApplication(description, driver)
        // 注册ApplicationInfo，将Application加入缓存，加入等待调度的队列
        registerApplication(app)
        logInfo("Registered app " + description.name + " with ID " + app.id)
        // 注册完成后使用持久化引擎，将ApplicationInfo进行持久化（zk或者文件系统）
        // 用于master recovery时恢复Application
        persistenceEngine.addApplication(app)
        // 反向向StandaloneSchedulerBacend的AppClient（即StandaloneAppClient）
        // 发送RegisteredApplication消息
        driver.send(RegisteredApplication(app.id, self))
        // 注册完成后参与资源调度
        schedule()
      }

    case ExecutorStateChanged(appId, execId, state, message, exitStatus) =>
      val execOption = idToApp.get(appId).flatMap(app => app.executors.get(execId))
      execOption match {
        case Some(exec) =>
          val appInfo = idToApp(appId)
          val oldState = exec.state
          exec.state = state

          if (state == ExecutorState.RUNNING) {
            assert(oldState == ExecutorState.LAUNCHING,
              s"executor $execId state transfer from $oldState to RUNNING is illegal")
            appInfo.resetRetryCount()
          }

          exec.application.driver.send(ExecutorUpdated(execId, state, message, exitStatus, false))

          if (ExecutorState.isFinished(state)) {
            // Remove this executor from the worker and app
            logInfo(s"Removing executor ${exec.fullId} because it is $state")
            // If an application has already finished, preserve its
            // state to display its information properly on the UI
            if (!appInfo.isFinished) {
              appInfo.removeExecutor(exec)
            }
            exec.worker.removeExecutor(exec)

            val normalExit = exitStatus == Some(0)
            // Only retry certain number of times so we don't go into an infinite loop.
            // Important note: this code path is not exercised by tests, so be very careful when
            // changing this `if` condition.
            if (!normalExit
                && appInfo.incrementRetryCount() >= MAX_EXECUTOR_RETRIES
                && MAX_EXECUTOR_RETRIES >= 0) { // < 0 disables this application-killing path
              val execs = appInfo.executors.values
              if (!execs.exists(_.state == ExecutorState.RUNNING)) {
                logError(s"Application ${appInfo.desc.name} with ID ${appInfo.id} failed " +
                  s"${appInfo.retryCount} times; removing it")
                removeApplication(appInfo, ApplicationState.FAILED)
              }
            }
          }
          schedule()
        case None =>
          logWarning(s"Got status update for unknown executor $appId/$execId")
      }

    case DriverStateChanged(driverId, state, exception) =>
      state match {
        // 判断driver的状态，如果是错误、完成、被杀掉、失败，则会move掉该driver
        case DriverState.ERROR | DriverState.FINISHED | DriverState.KILLED | DriverState.FAILED =>
          // 移除master上注册的Driver
          removeDriver(driverId, state, exception)
        case _ =>
          throw new Exception(s"Received unexpected state update for driver $driverId: $state")
      }

    case Heartbeat(workerId, worker) =>
      idToWorker.get(workerId) match {
        case Some(workerInfo) =>
          workerInfo.lastHeartbeat = System.currentTimeMillis()
        case None =>
          if (workers.map(_.id).contains(workerId)) {
            logWarning(s"Got heartbeat from unregistered worker $workerId." +
              " Asking it to re-register.")
            worker.send(ReconnectWorker(masterUrl))
          } else {
            logWarning(s"Got heartbeat from unregistered worker $workerId." +
              " This worker was never registered, so ignoring the heartbeat.")
          }
      }

    case MasterChangeAcknowledged(appId) =>
      idToApp.get(appId) match {
        case Some(app) =>
          logInfo("Application has been re-registered: " + appId)
          app.state = ApplicationState.WAITING
        case None =>
          logWarning("Master change ack from unknown app: " + appId)
      }

      if (canCompleteRecovery) { completeRecovery() }

    case WorkerSchedulerStateResponse(workerId, executors, driverIds) =>
      idToWorker.get(workerId) match {
        case Some(worker) =>
          logInfo("Worker has been re-registered: " + workerId)
          worker.state = WorkerState.ALIVE

          val validExecutors = executors.filter(exec => idToApp.get(exec.appId).isDefined)
          for (exec <- validExecutors) {
            val app = idToApp.get(exec.appId).get
            val execInfo = app.addExecutor(worker, exec.cores, Some(exec.execId))
            worker.addExecutor(execInfo)
            execInfo.copyState(exec)
          }

          for (driverId <- driverIds) {
            drivers.find(_.id == driverId).foreach { driver =>
              driver.worker = Some(worker)
              driver.state = DriverState.RUNNING
              worker.addDriver(driver)
            }
          }
        case None =>
          logWarning("Scheduler state from unknown worker: " + workerId)
      }

      if (canCompleteRecovery) { completeRecovery() }

    case WorkerLatestState(workerId, executors, driverIds) =>
      idToWorker.get(workerId) match {
        case Some(worker) =>
          for (exec <- executors) {
            val executorMatches = worker.executors.exists {
              case (_, e) => e.application.id == exec.appId && e.id == exec.execId
            }
            if (!executorMatches) {
              // master doesn't recognize this executor. So just tell worker to kill it.
              worker.endpoint.send(KillExecutor(masterUrl, exec.appId, exec.execId))
            }
          }

          for (driverId <- driverIds) {
            val driverMatches = worker.drivers.exists { case (id, _) => id == driverId }
            if (!driverMatches) {
              // master doesn't recognize this driver. So just tell worker to kill it.
              worker.endpoint.send(KillDriver(driverId))
            }
          }
        case None =>
          logWarning("Worker state from unknown worker: " + workerId)
      }

    case UnregisterApplication(applicationId) =>
      logInfo(s"Received unregister request from application $applicationId")
      idToApp.get(applicationId).foreach(finishApplication)

    case CheckForWorkerTimeOut =>
      timeOutDeadWorkers()

  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case RequestSubmitDriver(description) =>
      if (state != RecoveryState.ALIVE) {
        val msg = s"${Utils.BACKUP_STANDALONE_MASTER_PREFIX}: $state. " +
          "Can only accept driver submissions in ALIVE state."
        context.reply(SubmitDriverResponse(self, false, None, msg))
      } else {
        logInfo("Driver submitted " + description.command.mainClass)
        val driver = createDriver(description)
        persistenceEngine.addDriver(driver)
        waitingDrivers += driver
        drivers.add(driver)
        schedule()

        // TODO: It might be good to instead have the submission client poll the master to determine
        //       the current status of the driver. For now it's simply "fire and forget".

        context.reply(SubmitDriverResponse(self, true, Some(driver.id),
          s"Driver successfully submitted as ${driver.id}"))
      }

    case RequestKillDriver(driverId) =>
      if (state != RecoveryState.ALIVE) {
        val msg = s"${Utils.BACKUP_STANDALONE_MASTER_PREFIX}: $state. " +
          s"Can only kill drivers in ALIVE state."
        context.reply(KillDriverResponse(self, driverId, success = false, msg))
      } else {
        logInfo("Asked to kill driver " + driverId)
        val driver = drivers.find(_.id == driverId)
        driver match {
          case Some(d) =>
            if (waitingDrivers.contains(d)) {
              waitingDrivers -= d
              self.send(DriverStateChanged(driverId, DriverState.KILLED, None))
            } else {
              // We just notify the worker to kill the driver here. The final bookkeeping occurs
              // on the return path when the worker submits a state change back to the master
              // to notify it that the driver was successfully killed.
              d.worker.foreach { w =>
                w.endpoint.send(KillDriver(driverId))
              }
            }
            // TODO: It would be nice for this to be a synchronous response
            val msg = s"Kill request for $driverId submitted"
            logInfo(msg)
            context.reply(KillDriverResponse(self, driverId, success = true, msg))
          case None =>
            val msg = s"Driver $driverId has already finished or does not exist"
            logWarning(msg)
            context.reply(KillDriverResponse(self, driverId, success = false, msg))
        }
      }

    case RequestDriverStatus(driverId) =>
      if (state != RecoveryState.ALIVE) {
        val msg = s"${Utils.BACKUP_STANDALONE_MASTER_PREFIX}: $state. " +
          "Can only request driver status in ALIVE state."
        context.reply(
          DriverStatusResponse(found = false, None, None, None, Some(new Exception(msg))))
      } else {
        (drivers ++ completedDrivers).find(_.id == driverId) match {
          case Some(driver) =>
            context.reply(DriverStatusResponse(found = true, Some(driver.state),
              driver.worker.map(_.id), driver.worker.map(_.hostPort), driver.exception))
          case None =>
            context.reply(DriverStatusResponse(found = false, None, None, None, None))
        }
      }

    case RequestMasterState =>
      context.reply(MasterStateResponse(
        address.host, address.port, restServerBoundPort,
        workers.toArray, apps.toArray, completedApps.toArray,
        drivers.toArray, completedDrivers.toArray, state))

    case BoundPortsRequest =>
      context.reply(BoundPortsResponse(address.port, webUi.boundPort, restServerBoundPort))

    case RequestExecutors(appId, requestedTotal) =>
      context.reply(handleRequestExecutors(appId, requestedTotal))

    case KillExecutors(appId, executorIds) =>
      val formattedExecutorIds = formatExecutorIds(executorIds)
      context.reply(handleKillExecutors(appId, formattedExecutorIds))
  }

  override def onDisconnected(address: RpcAddress): Unit = {
    // The disconnected client could've been either a worker or an app; remove whichever it was
    logInfo(s"$address got disassociated, removing it.")
    addressToWorker.get(address).foreach(removeWorker(_, s"${address} got disassociated"))
    addressToApp.get(address).foreach(finishApplication)
    if (state == RecoveryState.RECOVERING && canCompleteRecovery) { completeRecovery() }
  }

  private def canCompleteRecovery =
    workers.count(_.state == WorkerState.UNKNOWN) == 0 &&
      apps.count(_.state == ApplicationState.UNKNOWN) == 0

  private def beginRecovery(storedApps: Seq[ApplicationInfo], storedDrivers: Seq[DriverInfo],
      storedWorkers: Seq[WorkerInfo]) {
    for (app <- storedApps) {
      logInfo("Trying to recover app: " + app.id)
      try {
        registerApplication(app)
        app.state = ApplicationState.UNKNOWN
        app.driver.send(MasterChanged(self, masterWebUiUrl))
      } catch {
        case e: Exception => logInfo("App " + app.id + " had exception on reconnect")
      }
    }

    for (driver <- storedDrivers) {
      // Here we just read in the list of drivers. Any drivers associated with now-lost workers
      // will be re-launched when we detect that the worker is missing.
      drivers += driver
    }

    for (worker <- storedWorkers) {
      logInfo("Trying to recover worker: " + worker.id)
      try {
        registerWorker(worker)
        worker.state = WorkerState.UNKNOWN
        worker.endpoint.send(MasterChanged(self, masterWebUiUrl))
      } catch {
        case e: Exception => logInfo("Worker " + worker.id + " had exception on reconnect")
      }
    }
  }

  private def completeRecovery() {
    // Ensure "only-once" recovery semantics using a short synchronization period.
    if (state != RecoveryState.RECOVERING) { return }
    state = RecoveryState.COMPLETING_RECOVERY

    // Kill off any workers and apps that didn't respond to us.
    workers.filter(_.state == WorkerState.UNKNOWN).foreach(
      removeWorker(_, "Not responding for recovery"))
    apps.filter(_.state == ApplicationState.UNKNOWN).foreach(finishApplication)

    // Update the state of recovered apps to RUNNING
    apps.filter(_.state == ApplicationState.WAITING).foreach(_.state = ApplicationState.RUNNING)

    // Reschedule drivers which were not claimed by any workers
    drivers.filter(_.worker.isEmpty).foreach { d =>
      logWarning(s"Driver ${d.id} was not found after master recovery")
      if (d.desc.supervise) {
        logWarning(s"Re-launching ${d.id}")
        relaunchDriver(d)
      } else {
        removeDriver(d.id, DriverState.ERROR, None)
        logWarning(s"Did not re-launch ${d.id} because it was not supervised")
      }
    }

    state = RecoveryState.ALIVE
    schedule()
    logInfo("Recovery complete - resuming operations!")
  }

  /**
   * Schedule executors to be launched on the workers.
   * Returns an array containing number of cores assigned to each worker.
   *
   * There are two modes of launching executors. The first attempts to spread out an application's
   * executors on as many workers as possible, while the second does the opposite (i.e. launch them
   * on as few workers as possible). The former is usually better for data locality purposes and is
   * the default.
   *
   * The number of cores assigned to each executor is configurable. When this is explicitly set,
   * multiple executors from the same application may be launched on the same worker if the worker
   * has enough cores and memory. Otherwise, each executor grabs all the cores available on the
   * worker by default, in which case only one executor per application may be launched on each
   * worker during one single schedule iteration.
   * Note that when `spark.executor.cores` is not set, we may still launch multiple executors from
   * the same application on the same worker. Consider appA and appB both have one executor running
   * on worker1, and appA.coresLeft > 0, then appB is finished and release all its cores on worker1,
   * thus for the next schedule iteration, appA launches a new executor that grabs all the free
   * cores on worker1, therefore we get multiple executors from appA running on worker1.
   *
   * It is important to allocate coresPerExecutor on each worker at a time (instead of 1 core
   * at a time). Consider the following example: cluster has 4 workers with 16 cores each.
   * User requests 3 executors (spark.cores.max = 48, spark.executor.cores = 16). If 1 core is
   * allocated at a time, 12 cores from each worker would be assigned to each executor.
   * Since 12 < 16, no executors would launch [SPARK-8881].
   */
  /**
    * * 在Workers上启动调度的Executors
    * * 返回每个Worker所需要调用的cores的array集合
    *
    * * 【【有两种启动方式】】
    * * 一种是spreadOutApps，他尝试着分配一个Application的Executors到尽量多的Workers上边；
    * * 另一种是非spreadOutApps，它分配到一个Application的Executors到尽量少的Workers上去；
    * *
    * * 前者通常更适合数据本地化的目的，并且它是默认的方式
    * *
    * * 分配给每个executor的内核数是可配置的
    * * 当明确配置的时候，当这个Worker有足够的核数与内存的情况下,来自同一个Application的多个Executors可能在相同的Worker上被启动
    * * 否则，默认情况下，每一个Executor会捕获Worker上所有可用的核数，在这种情况下，在每一个Worker上可能只会启动一个Executor。
    * *
    * * 一次性分配每个Executor所需要的cores到每一个Worker上边很重要 [代替每次分配一个core]
    * * 【即需要一个Executor只能用一个Worker的资源】
    * * 假设：集群有4个Worker，每个Worker16核；要求3个Executor，每个Executor需要16核；
    * * 如果一个core一次，则需要从每个Worker上取出12个core分配给每一个Executor
    * * 由于 12 < 16 ，将没有Executor被启动。
    */
  /**
  举例说明
  每一个application至少包含以下基本属性:
  coresPerExecutor：每一个Executor进程的cpu cores个数
  memoryPerExecutor：每一个Executor进程的memory大小
  maxCores: 这个application最多需要的cpu cores个数。

  每一个worker至少包含以下基本属性：
  freeCores:worker 节点当前可用的cpu cores个数
  memoryFree:worker节点当前可用的memory大小。

  假设一个待注册的application如下：
  coresPerExecutor：2
  memoryPerExecutor：512M
  maxCores: 12
  这表示这个application 最多需要12个cpu cores，每一个Executor都要2个core，512M内存。

  假设某一时刻spark集群有如下几个worker节点,他们按照coresFree降序排列:
  Worker1:coresFree=10  memoryFree=10G
  Worker2:coresFree=7   memoryFree=1G
  Worker3:coresFree=3   memoryFree=2G
  Worker4:coresFree=2   memoryFree=215M
  Worker5:coresFree=1   memoryFree=1G

  其中worker5不满足application的要求：worker5.coresFree < application.coresPerExecutor
  worker4也不满足application的要求:worker4.memoryFree < application.memoryPerExecutor
  因此最终满足调度要求的worker节点只有前三个，我们将这三个节点记作usableWorkers。

  SPREADOUT算法
  先介绍spreadOut算法吧。上面已经说了，满足条件的worker只有前三个:
  Worker1:coresFree=10  memoryFree=10G
  Worker2:coresFree=7   memoryFree=1G
  Worker3:coresFree=3   memoryFree=2G

  第一次调度之后，worker列表如下:
  Worker1:coresFree=8  memoryFree=9.5G  assignedExecutors=1  assignedCores=2
  Worker2:coresFree=7  memoryFree=1G    assignedExecutors=0  assignedCores=0
  Worker3:coresFree=3  memoryFree=2G    assignedExecutors=0  assignedCores=0
  totalExecutors:1,totalCores=2
  可以发现，worker1的coresFree和memoryFree都变小了而worker2，worker3并没有发生改变，这是因为
  我们在worker1上面分配了一个Executor进程(这个Executor进程占用2个cpu cores，512M memory)而没
  有在workre2和worker3上分配。

  接下来继续循环，开始去worker2上分配:
  Worker1:coresFree=8  memoryFree=9.5G      assignedExecutors=1  assignedCores=2
  Worker2:coresFree=5  memoryFree=512M      assignedExecutors=1  assignedCores=2
  Worker3:coresFree=3  memoryFree=2G        assignedExecutors=0  assignedCores=0
  totalExecutors:2,totalCores=4
  此时已经分配了2个Executor进程，4个core。

  接下来去worker3上分配:
  Worker1:coresFree=8  memoryFree=9.5G      assignedExecutors=1  assignedCores=2
  Worker2:coresFree=5  memoryFree=512M      assignedExecutors=1  assignedCores=2
  Worker3:coresFree=1  memoryFree=1.5G      assignedExecutors=1  assignedCores=2
  totalExecutors:3,totalCores=6

  接下来再去worker1分配，然后worker2...依此类推...以round-robin方式分配，
  由于worker3.coresFree < application.coresPerExecutor,不会在它上面分配资源了：
  Worker1:coresFree=6  memoryFree=9.0G      assignedExecutors=2  assignedCores=4
  Worker2:coresFree=5  memoryFree=512M      assignedExecutors=1  assignedCores=2
  Worker3:coresFree=1  memoryFree=1.5G      assignedExecutors=1  assignedCores=2
  totalExecutors:4,totalCores=8

  Worker1:coresFree=6  memoryFree=9.0G      assignedExecutors=2  assignedCores=4
  Worker2:coresFree=3  memoryFree=0M        assignedExecutors=2  assignedCores=4
  Worker3:coresFree=1  memoryFree=1.5G      assignedExecutors=1  assignedCores=2
  totalExecutors:5,totalCores=10
  此时worker2也不满足要求了：worker2.memoryFree < application.memoryPerExecutor
  因此，下一次分配就去worker1上了：
  Worker1:coresFree=4  memoryFree=8.5G      assignedExecutors=3  assignedCores=6
  Worker2:coresFree=3  memoryFree=0M        assignedExecutors=2  assignedCores=4
  Worker3:coresFree=1  memoryFree=1.5G      assignedExecutors=1  assignedCores=2
  totalExecutors:6,totalCores=12
  ok，由于已经分配了12个core，达到了application的要求，所以不在为这个application调度了。

  非SPREADOUT算法
  那么非spraadOut算法呢？他是逮到一个worker如果不把他的资源耗尽了是不会放手的：
  第一次调度，结果如下：
  Worker1:coresFree=8  memoryFree=9.5G  assignedExecutors=1  assignedCores=2
  Worker2:coresFree=7  memoryFree=1G    assignedExecutors=0  assignedCores=0
  Worker3:coresFree=3  memoryFree=2G    assignedExecutors=0  assignedCores=0
  totalExecutors:1,totalCores=2

  第二次调度，结果如下：
  Worker1:coresFree=6  memoryFree=9.0G  assignedExecutors=2  assignedCores=4
  Worker2:coresFree=7  memoryFree=1G    assignedExecutors=0  assignedCores=0
  Worker3:coresFree=3  memoryFree=2G    assignedExecutors=0  assignedCores=0
  totalExecutors:2,totalCores=4

  第三次调度，结果如下：
  Worker1:coresFree=4  memoryFree=8.5    assignedExecutors=3  assignedCores=6
  Worker2:coresFree=7  memoryFree=1G     assignedExecutors=0  assignedCores=0
  Worker3:coresFree=3  memoryFree=2G     assignedExecutors=0  assignedCores=0
  totalExecutors:3,totalCores=6

  第四次调度，结果如下：
  Worker1:coresFree=2   memoryFree=8.0G  assignedExecutors=4  assignedCores=8
  Worker2:coresFree=7   memoryFree=1G    assignedExecutors=0  assignedCores=0
  Worker3:coresFree=3   memoryFree=2G    assignedExecutors=0  assignedCores=0
  totalExecutors:4,totalCores=8

  第五次调度，结果如下：
  Worker1:coresFree=0   memoryFree=7.5G  assignedExecutors=5  assignedCores=10
  Worker2:coresFree=7   memoryFree=1G    assignedExecutors=0  assignedCores=0
  Worker3:coresFree=3   memoryFree=2G    assignedExecutors=0  assignedCores=0
  totalExecutors:5,totalCores=10
  当worker1的coresfree已经耗尽了。由于application需要12个core，而这里才分配了10个，所以还要继续往下分配：

  第五次调度，结果如下：
  Worker1:coresFree=0   memoryFree=7.5G      assignedExecutors=5  assignedCores=10
  Worker2:coresFree=5   memoryFree=512G      assignedExecutors=1  assignedCores=2
  Worker3:coresFree=3   memoryFree=2G        assignedExecutors=0  assignedCores=0
  totalExecutors:6,totalCores=12
  ok，最终分配来12个core，满足了application的要求。

  对比：
  spreadOut算法中，是以round-robin方式，轮询的在worker节点上分配Executor进程，即以如下序列分配:
  worker1,worker2... ... worker n,worker1... .... worker n
  非spreadOut算法中，逮者一个worker就不放手，直到满足一下条件之一：
  worker.freeCores < application.coresPerExecutor
  worker.memoryFree < application.memoryPerExecutor
  在上面两个例子中，虽然最终都分配了6个Executor进程和12个core，但是spreadOut方式下，6个Executor进程分散
  在不同的worker节点上，充分利用了spark集群的worker节点，而非spreadOut方式下，只在worker1和worker2上分配
  了Executor进程，并没有充分利用spark worker节点。

  另外一种模式：SPREADOUT + ONEEXECUTORPERWORKER 算法
  oneExecutorPerWorker = coresPerExecutor.isEmpty为true，即coresPerExecutor未定义
  此时启动”oneExecutorPerWorker“机制，该机制下一个worker上只启动一个Executor进程，该进程包含所有分配的cores，
  每一次调度给Executor分配一个core，并且onExecutorPerWorker机制不检查内存的限制。
    */
  private def scheduleExecutorsOnWorkers(
      app: ApplicationInfo,
      usableWorkers: Array[WorkerInfo],
      spreadOutApps: Boolean): Array[Int] = {
    // 配置中每个Executor需要core数量
    val coresPerExecutor = app.desc.coresPerExecutor
    // 每个Executor的最小核数，没配置即为1核
    val minCoresPerExecutor: Int = coresPerExecutor.getOrElse(1)
    // 判断配置中的每个Executor的cores是否为空
    // 【如果为空，则表示每个Executor只用分配一个core
    // （如果oneExecutorPerWorker为true那么代表minCoresPerExecutor为1，
    // 且对于当前的application只能在一个worker上启动该app的一个executor进程）后续判断，
    // 并且oneExecutorPerWorker机制不检查内存的限制】
    val oneExecutorPerWorker = coresPerExecutor.isEmpty
    // 配置中每个Executor的内存
    val memoryPerExecutor = app.desc.memoryPerExecutorMB
    // 可用worker的数量
    val numUsable = usableWorkers.length
    // 给每个worker分配的核数
    val assignedCores = new Array[Int](numUsable) // Number of cores to give to each worker
    // 每个Worker上分配的Executor的个数的集合
    val assignedExecutors = new Array[Int](numUsable) // Number of new executors on each worker
    // 可以分配的核数【取Application剩余所需要的核数 与 所有Worker空余的核数总和的最小值】
    var coresToAssign = math.min(app.coresLeft, usableWorkers.map(_.coresFree).sum)

    /** Return whether the specified worker can launch an executor for this app. */
    // 判断指定的worker剩余资源是否可以为这个Application启动一个Executor
    def canLaunchExecutor(pos: Int): Boolean = {
      // 判断是否继续调度：可以分配的核数 >= 每个Executor所需要的最小核数
      val keepScheduling = coresToAssign >= minCoresPerExecutor
      // 判断是否有足够的核数
      // 该worker空余的核数 - 该worker已经分配的要用的core核数 >= 一个Executor所需要的最小核数
      val enoughCores = usableWorkers(pos).coresFree - assignedCores(pos) >= minCoresPerExecutor

      // If we allow multiple executors per worker, then we can always launch new executors.
      // Otherwise, if there is already an executor on this worker, just give it more cores.
      // 在这个Worker上没有启动Executor，或者一个Executor上需要启动多个cores。
      // oneExecutorPerWorker=false,则说明一个Executor上需要启动多个cores
      val launchingNewExecutor = !oneExecutorPerWorker || assignedExecutors(pos) == 0
      if (launchingNewExecutor) {
        // worker已经分配的memory，该worker上启动的Executors个数 * 每个Executor的内存
        val assignedMemory = assignedExecutors(pos) * memoryPerExecutor
        // 判断是否有足够的memory来启动Executor：
        // 该worker空余的memory - 该worker已经分配给Executor的memory >= 一个Executor所需要的最小memory
        val enoughMemory = usableWorkers(pos).memoryFree - assignedMemory >= memoryPerExecutor

        // 这里做安全判断
        // 要分配启动的Executor和当前application启动的使用的Executor总数是否在Application总的Executor限制之下
        val underLimit = assignedExecutors.sum + app.executors.size < app.executorLimit
        keepScheduling && enoughCores && enoughMemory && underLimit
      } else {
        // We're adding cores to an existing executor, so no need
        // to check memory and executor limits
        // 由于一个worker启动一个executor
        // 后续分配我们将worker分配的cores添加到该executor，
        // 并且不需要检查内存对executor的限制
        keepScheduling && enoughCores
      }
    }

    // Keep launching executors until no more workers can accommodate any
    // more executors, or if we have reached this application's limits
    // 过滤出来至少可以提交一个Executor的workers
    var freeWorkers = (0 until numUsable).filter(canLaunchExecutor)
    // 一直提交Executor，直到没有可用的Worker 或者是到达了Application所需要的的Executor的上限
    while (freeWorkers.nonEmpty) {
      freeWorkers.foreach { pos =>
        var keepScheduling = true
        while (keepScheduling && canLaunchExecutor(pos)) {
          // 循环一次重新计算可分配的核数
          coresToAssign -= minCoresPerExecutor
          // 如果每个Worker只启动一个Executor(即minCoresPerExecutor的值为1)
          // 那么每一次循环给这个Executor分配一个core
          assignedCores(pos) += minCoresPerExecutor

          // If we are launching one executor per worker, then every iteration assigns 1 core
          // to the executor. Otherwise, every iteration assigns cores to a new executor.
          // 如果oneExecutorPerWorker为真，那么每个Worker只启动一个Executor ，否则调度一次启动一个Executor
          if (oneExecutorPerWorker) {
            assignedExecutors(pos) = 1
          } else {
            assignedExecutors(pos) += 1
          }

          // Spreading out an application means spreading out its executors across as
          // many workers as possible. If we are not spreading out, then we should keep
          // scheduling executors on this worker until we use all of its resources.
          // Otherwise, just move on to the next worker.
          // spreadOutApps：尽量分配Executors 到最多的Worker上；
          // 非spreadOutApps ：紧着一个Worker分配Executors，直到这个Worker的资源被用尽。
          if (spreadOutApps) {
            keepScheduling = false
          }
        }
      }
      // 每一次循环，都进行一次过滤，过滤出来仍然可以提交一个Executor的workers
      freeWorkers = freeWorkers.filter(canLaunchExecutor)
    }
    // 返回，每个Worker所需要调用的cores的集合
    assignedCores
  }

  /**
   * Schedule and launch executors on workers
    * 在Worker上 调度启动Executors
    * 这是一个简单的FIFO调度
    * 一直尝试着在队列中装配好第一个Application之后，紧接着装配第二个...以此类推
   */
  private def startExecutorsOnWorkers(): Unit = {
    // Right now this is a very simple FIFO scheduler. We keep trying to fit in the first app
    // in the queue, then the second app, etc.
    // 遍历waitingApps中的ApplicationInfo，进行Application的资源调度
    for (app <- waitingApps) {
      // 使用--executor-cores或spark.executor.cores属性指定核心数coresPerExecutor。
      // 使用--executor-memory参数或spark.executor.memory属性配置堆大小
      // coresPerExecutor未设置则默认为1个core
      val coresPerExecutor = app.desc.coresPerExecutor.getOrElse(1)
      // If the cores left is less than the coresPerExecutor,the cores left will not be allocated
      // 调度的Application剩余需要的core数量大于等于coresPerExecutor时进行调度
      // 即分配Executor之前要判断应用程序是否還需要分配Core如果不需要則不会為应用程序分配Executor
      // 如果调度的Application剩余需要的core数量小于executor需要的core数量，
      // 则Applicatioon剩余的core将不会被分配。
      // coresLeft=当前app申请的maxcpus - granted的cpus，
      // maxcpus由参数spark.cores.max配置文件设置决定
      if (app.coresLeft >= coresPerExecutor) {
        // Filter out workers that don't have enough resources to launch an executor
        // 过滤掉存活但是剩余core/内存不足以启动一个Executor的worker
        // （即worker的memoryFree小于memoryPerExecutorMB或者coresFree小于coresPerExecutor）
        // 剩余的worker参与Application的Executor资源分配，即usableWorkers，然后按照coresFree倒序排序
        val usableWorkers = workers.toArray.filter(_.state == WorkerState.ALIVE)
          .filter(worker => worker.memoryFree >= app.desc.memoryPerExecutorMB &&
            worker.coresFree >= coresPerExecutor)
          .sortBy(_.coresFree).reverse
        // Application的调度算法有两种，一种是spreadOutApps，另一种是非spreadOutApps
        // spreadOutApps通过配置文件spark.deploy.spreadOut定义，默认为ture
        // assignedCores为每个Worker所需要调用的cores的array集合
        // 调用scheduleExecutorsOnWorkers方法进行Executor资源分配
        val assignedCores = scheduleExecutorsOnWorkers(app, usableWorkers, spreadOutApps)

        // Now that we've decided how many cores to allocate on each worker, let's allocate them
        // 通过上述过程，我们明确了每个worker分配多少cores给applicatuon，
        // 循环前边判断后分配了core的worker，该出分配core给application
        for (pos <- 0 until usableWorkers.length if assignedCores(pos) > 0) {
          // 通过调用allocateWorkerResourceToExecutors方法分配worker的cores
          allocateWorkerResourceToExecutors(
            app, assignedCores(pos), app.desc.coresPerExecutor, usableWorkers(pos))
        }
      }
    }
  }

  /**
   * Allocate a worker's resources to one or more executors.
   * @param app the info of the application which the executors belong to
   * @param assignedCores number of cores on this worker for this application
   * @param coresPerExecutor number of cores per executor
   * @param worker the worker info
   */
  private def allocateWorkerResourceToExecutors(
      app: ApplicationInfo,
      assignedCores: Int,
      coresPerExecutor: Option[Int],
      worker: WorkerInfo): Unit = {
    // If the number of cores per executor is specified, we divide the cores assigned
    // to this worker evenly among the executors with no remainder.
    // Otherwise, we launch a single executor that grabs all the assignedCores on this worker.
    // 如果每一个Executor所需的core的数量被配置(coresPerExecutor配置有值)，我们均匀的分配这个worker的cores给每一个Executor。
    // 否则的话，我们仅仅启动一个Executor，它占用这个Worker的所有被分配出来的cores
    val numExecutors = coresPerExecutor.map { assignedCores / _ }.getOrElse(1)
    // 每个Executor所拥有的cores，等于coresPerExecutor的值或者分配的全部cores
    val coresToAssign = coresPerExecutor.getOrElse(assignedCores)
    for (i <- 1 to numExecutors) {
      // 添加Executor的信息 返回这个executor，调用ApplicationInfo的addExecutor方法
      val exec = app.addExecutor(worker, coresToAssign)
      // 在Worker上注册启动Executor
      launchExecutor(worker, exec)
      // 变更Application的状态为 RUNNING
      app.state = ApplicationState.RUNNING
    }
  }

  /**
   * Schedule the currently available resources among waiting apps. This method will be called
   * every time a new app joins or resource availability changes.
   */
  private def schedule(): Unit = {
    // 判断master的状态不是alive的话直接返回
    // 也就是说stadby master不会进行application等资源的调度
    if (state != RecoveryState.ALIVE) {
      return
    }
    // Drivers take strict precedence over executors
    // 取出workers中之前所有注册状态仍为ALIVE的worker，并调用Random的shuffle方法进行随机打乱
    val shuffledAliveWorkers = Random.shuffle(workers.toSeq.filter(_.state == WorkerState.ALIVE))
    // 获取状态为alive的worker的数量，即当前可用worker的数量
    val numWorkersAlive = shuffledAliveWorkers.size
    var curPos = 0
    // 首先，针对driver进行调度，什么情况下才会进行driver的注册？
    // 只有用yarn-cluster模式提交的时候才会注册driver
    // 因为stanalone和yarn-client模式，都会在本地直接启动driver，而不会来注册driver，就更不可能让master调度driver了
    // 遍历waitingDrivers中所有driver进行资源调度
    for (driver <- waitingDrivers.toList) { // iterate over a copy of waitingDrivers
      // We assign workers to each waiting driver in a round-robin fashion. For each driver, we
      // start from the last worker that was assigned a driver, and continue onwards until we have
      // explored all alive workers.
      var launched = false
      var numWorkersVisited = 0
      // 只要还有活着的worker没有遍历到
      // 并且当前的driver还没有启动，那么就循环遍历worker
      while (numWorkersVisited < numWorkersAlive && !launched) {
        val worker = shuffledAliveWorkers(curPos)
        numWorkersVisited += 1
        // 如果当前worker的内存空闲量大于等于driver需要的内存
        // 并且当前worker的cpu数量，大于等于driver所需要的cpu数量
        if (worker.memoryFree >= driver.desc.mem && worker.coresFree >= driver.desc.cores) {
          // 那么就启动driver
          launchDriver(worker, driver)
          // 将driver从waitingDrivers队列中移除，后续的schedule将不会调度该driver
          waitingDrivers -= driver
          // 设置launched变量，退出while循环
          launched = true
        }
        // 将指针指向下一个worker
        curPos = (curPos + 1) % numWorkersAlive
      }
    }
    // 在Worker上 调度启动Executors
    startExecutorsOnWorkers()
  }

  private def launchExecutor(worker: WorkerInfo, exec: ExecutorDesc): Unit = {
    logInfo("Launching executor " + exec.fullId + " on worker " + worker.id)
    // 更新worker资源使用情况
    // 内存加上分配给该worker上启动Executor的内存
    // cpu加上分配给该worker上启动Executor的cpu
    worker.addExecutor(exec)
    // 向worker发送LaunchExecutor消息
    worker.endpoint.send(LaunchExecutor(masterUrl,
      exec.application.id, exec.id, exec.application.desc, exec.cores, exec.memory))
    // Executor启动成功后回反向注册
    // 向driver（StandaloneAppClient）发送ExecutorAdded消息
    // 实际上是发送给ClientEndpoint这个消息通讯体
    // ClientEndpoint在StandaloneAppClient的start方法中初始化
    exec.application.driver.send(
      ExecutorAdded(exec.id, worker.id, worker.hostPort, exec.cores, exec.memory))
  }

  private def registerWorker(worker: WorkerInfo): Boolean = {
    // There may be one or more refs to dead workers on this same node (w/ different ID's),
    // remove them.
    workers.filter { w =>
      (w.host == worker.host && w.port == worker.port) && (w.state == WorkerState.DEAD)
    }.foreach { w =>
      workers -= w
    }

    val workerAddress = worker.endpoint.address
    if (addressToWorker.contains(workerAddress)) {
      val oldWorker = addressToWorker(workerAddress)
      if (oldWorker.state == WorkerState.UNKNOWN) {
        // A worker registering from UNKNOWN implies that the worker was restarted during recovery.
        // The old worker must thus be dead, so we will remove it and accept the new worker.
        removeWorker(oldWorker, "Worker replaced by a new worker with same address")
      } else {
        logInfo("Attempted to re-register worker at same address: " + workerAddress)
        return false
      }
    }

    workers += worker
    idToWorker(worker.id) = worker
    addressToWorker(workerAddress) = worker
    true
  }

  private def removeWorker(worker: WorkerInfo, msg: String) {
    logInfo("Removing worker " + worker.id + " on " + worker.host + ":" + worker.port)
    worker.setState(WorkerState.DEAD)
    idToWorker -= worker.id
    addressToWorker -= worker.endpoint.address

    for (exec <- worker.executors.values) {
      logInfo("Telling app of lost executor: " + exec.id)
      exec.application.driver.send(ExecutorUpdated(
        exec.id, ExecutorState.LOST, Some("worker lost"), None, workerLost = true))
      exec.state = ExecutorState.LOST
      exec.application.removeExecutor(exec)
    }
    for (driver <- worker.drivers.values) {
      if (driver.desc.supervise) {
        logInfo(s"Re-launching ${driver.id}")
        relaunchDriver(driver)
      } else {
        logInfo(s"Not re-launching ${driver.id} because it was not supervised")
        removeDriver(driver.id, DriverState.ERROR, None)
      }
    }
    logInfo(s"Telling app of lost worker: " + worker.id)
    apps.filterNot(completedApps.contains(_)).foreach { app =>
      app.driver.send(WorkerRemoved(worker.id, worker.host, msg))
    }
    persistenceEngine.removeWorker(worker)
  }

  private def relaunchDriver(driver: DriverInfo) {
    // We must setup a new driver with a new driver id here, because the original driver may
    // be still running. Consider this scenario: a worker is network partitioned with master,
    // the master then relaunches driver driverID1 with a driver id driverID2, then the worker
    // reconnects to master. From this point on, if driverID2 is equal to driverID1, then master
    // can not distinguish the statusUpdate of the original driver and the newly relaunched one,
    // for example, when DriverStateChanged(driverID1, KILLED) arrives at master, master will
    // remove driverID1, so the newly relaunched driver disappears too. See SPARK-19900 for details.
    removeDriver(driver.id, DriverState.RELAUNCHING, None)
    val newDriver = createDriver(driver.desc)
    persistenceEngine.addDriver(newDriver)
    drivers.add(newDriver)
    waitingDrivers += newDriver

    schedule()
  }

  private def createApplication(desc: ApplicationDescription, driver: RpcEndpointRef):
      ApplicationInfo = {
    val now = System.currentTimeMillis()
    val date = new Date(now)
    val appId = newApplicationId(date)
    new ApplicationInfo(now, appId, desc, date, driver, defaultCores)
  }

  private def registerApplication(app: ApplicationInfo): Unit = {
    // 获取到Application对应driver的ip地址
    val appAddress = app.driver.address
    // 如果ip地址存在，则认为重复注册，直接返回
    if (addressToApp.contains(appAddress)) {
      logInfo("Attempted to re-register application at same address: " + appAddress)
      return
    }

    applicationMetricsSystem.registerSource(app.appSource)
    // 这里其实就是将app的信息加入内存缓存中
    apps += app
    idToApp(app.id) = app
    endpointToApp(app.driver) = app
    addressToApp(appAddress) = app
    // 将app加入等待调度的队列--waitingApps
    waitingApps += app
  }

  private def finishApplication(app: ApplicationInfo) {
    removeApplication(app, ApplicationState.FINISHED)
  }

  def removeApplication(app: ApplicationInfo, state: ApplicationState.Value) {
    if (apps.contains(app)) {
      logInfo("Removing app " + app.id)
      apps -= app
      idToApp -= app.id
      endpointToApp -= app.driver
      addressToApp -= app.driver.address

      if (completedApps.size >= RETAINED_APPLICATIONS) {
        val toRemove = math.max(RETAINED_APPLICATIONS / 10, 1)
        completedApps.take(toRemove).foreach { a =>
          applicationMetricsSystem.removeSource(a.appSource)
        }
        completedApps.trimStart(toRemove)
      }
      completedApps += app // Remember it in our history
      waitingApps -= app

      for (exec <- app.executors.values) {
        killExecutor(exec)
      }
      app.markFinished(state)
      if (state != ApplicationState.FINISHED) {
        app.driver.send(ApplicationRemoved(state.toString))
      }
      persistenceEngine.removeApplication(app)
      schedule()

      // Tell all workers that the application has finished, so they can clean up any app state.
      workers.foreach { w =>
        w.endpoint.send(ApplicationFinished(app.id))
      }
    }
  }

  /**
   * Handle a request to set the target number of executors for this application.
   *
   * If the executor limit is adjusted upwards, new executors will be launched provided
   * that there are workers with sufficient resources. If it is adjusted downwards, however,
   * we do not kill existing executors until we explicitly receive a kill request.
   *
   * @return whether the application has previously registered with this Master.
   */
  private def handleRequestExecutors(appId: String, requestedTotal: Int): Boolean = {
    idToApp.get(appId) match {
      case Some(appInfo) =>
        logInfo(s"Application $appId requested to set total executors to $requestedTotal.")
        appInfo.executorLimit = requestedTotal
        schedule()
        true
      case None =>
        logWarning(s"Unknown application $appId requested $requestedTotal total executors.")
        false
    }
  }

  /**
   * Handle a kill request from the given application.
   *
   * This method assumes the executor limit has already been adjusted downwards through
   * a separate [[RequestExecutors]] message, such that we do not launch new executors
   * immediately after the old ones are removed.
   *
   * @return whether the application has previously registered with this Master.
   */
  private def handleKillExecutors(appId: String, executorIds: Seq[Int]): Boolean = {
    idToApp.get(appId) match {
      case Some(appInfo) =>
        logInfo(s"Application $appId requests to kill executors: " + executorIds.mkString(", "))
        val (known, unknown) = executorIds.partition(appInfo.executors.contains)
        known.foreach { executorId =>
          val desc = appInfo.executors(executorId)
          appInfo.removeExecutor(desc)
          killExecutor(desc)
        }
        if (unknown.nonEmpty) {
          logWarning(s"Application $appId attempted to kill non-existent executors: "
            + unknown.mkString(", "))
        }
        schedule()
        true
      case None =>
        logWarning(s"Unregistered application $appId requested us to kill executors!")
        false
    }
  }

  /**
   * Cast the given executor IDs to integers and filter out the ones that fail.
   *
   * All executors IDs should be integers since we launched these executors. However,
   * the kill interface on the driver side accepts arbitrary strings, so we need to
   * handle non-integer executor IDs just to be safe.
   */
  private def formatExecutorIds(executorIds: Seq[String]): Seq[Int] = {
    executorIds.flatMap { executorId =>
      try {
        Some(executorId.toInt)
      } catch {
        case e: NumberFormatException =>
          logError(s"Encountered executor with a non-integer ID: $executorId. Ignoring")
          None
      }
    }
  }

  /**
   * Ask the worker on which the specified executor is launched to kill the executor.
   */
  private def killExecutor(exec: ExecutorDesc): Unit = {
    exec.worker.removeExecutor(exec)
    exec.worker.endpoint.send(KillExecutor(masterUrl, exec.application.id, exec.id))
    exec.state = ExecutorState.KILLED
  }

  /** Generate a new app ID given an app's submission date */
  private def newApplicationId(submitDate: Date): String = {
    val appId = "app-%s-%04d".format(createDateFormat.format(submitDate), nextAppNumber)
    nextAppNumber += 1
    appId
  }

  /** Check for, and remove, any timed-out workers */
  private def timeOutDeadWorkers() {
    // Copy the workers into an array so we don't modify the hashset while iterating through it
    val currentTime = System.currentTimeMillis()
    val toRemove = workers.filter(_.lastHeartbeat < currentTime - WORKER_TIMEOUT_MS).toArray
    for (worker <- toRemove) {
      if (worker.state != WorkerState.DEAD) {
        logWarning("Removing %s because we got no heartbeat in %d seconds".format(
          worker.id, WORKER_TIMEOUT_MS / 1000))
        removeWorker(worker, s"Not receiving heartbeat for ${WORKER_TIMEOUT_MS / 1000} seconds")
      } else {
        if (worker.lastHeartbeat < currentTime - ((REAPER_ITERATIONS + 1) * WORKER_TIMEOUT_MS)) {
          workers -= worker // we've seen this DEAD worker in the UI, etc. for long enough; cull it
        }
      }
    }
  }

  private def newDriverId(submitDate: Date): String = {
    val appId = "driver-%s-%04d".format(createDateFormat.format(submitDate), nextDriverNumber)
    nextDriverNumber += 1
    appId
  }

  private def createDriver(desc: DriverDescription): DriverInfo = {
    val now = System.currentTimeMillis()
    val date = new Date(now)
    new DriverInfo(now, newDriverId(date), desc, date)
  }

  private def launchDriver(worker: WorkerInfo, driver: DriverInfo) {
    logInfo("Launching driver " + driver.id + " on worker " + worker.id)
    // 将driver加入worker的内存缓存结构
    // 将worker内使用的内存和cpu数量都加上driver需要的内存和cpu数量
    worker.addDriver(driver)
    driver.worker = Some(worker)
    // 向Worker发送LaunchDriver消息，让worker来启动driver
    worker.endpoint.send(LaunchDriver(driver.id, driver.desc))
    // 完成后设置driver的状态为running
    driver.state = DriverState.RUNNING
  }

  private def removeDriver(
      driverId: String,
      finalState: DriverState,
      exception: Option[Exception]) {
    drivers.find(d => d.id == driverId) match {
      case Some(driver) =>
        logInfo(s"Removing driver: $driverId")
        drivers -= driver
        if (completedDrivers.size >= RETAINED_DRIVERS) {
          val toRemove = math.max(RETAINED_DRIVERS / 10, 1)
          completedDrivers.trimStart(toRemove)
        }
        completedDrivers += driver
        persistenceEngine.removeDriver(driver)
        driver.state = finalState
        driver.exception = exception
        driver.worker.foreach(w => w.removeDriver(driver))
        schedule()
      case None =>
        logWarning(s"Asked to remove unknown driver: $driverId")
    }
  }
}

private[deploy] object Master extends Logging {
  val SYSTEM_NAME = "sparkMaster"
  val ENDPOINT_NAME = "Master"

  def main(argStrings: Array[String]) {
    Thread.setDefaultUncaughtExceptionHandler(new SparkUncaughtExceptionHandler(
      exitOnUncaughtException = false))
    Utils.initDaemon(log)
    val conf = new SparkConf
    val args = new MasterArguments(argStrings, conf)
    val (rpcEnv, _, _) = startRpcEnvAndEndpoint(args.host, args.port, args.webUiPort, conf)
    rpcEnv.awaitTermination()
  }

  /**
   * Start the Master and return a three tuple of:
   *   (1) The Master RpcEnv
   *   (2) The web UI bound port
   *   (3) The REST server bound port, if any
   */
  def startRpcEnvAndEndpoint(
      host: String,
      port: Int,
      webUiPort: Int,
      conf: SparkConf): (RpcEnv, Int, Option[Int]) = {
    val securityMgr = new SecurityManager(conf)
    val rpcEnv = RpcEnv.create(SYSTEM_NAME, host, port, conf, securityMgr)
    val masterEndpoint = rpcEnv.setupEndpoint(ENDPOINT_NAME,
      new Master(rpcEnv, rpcEnv.address, webUiPort, securityMgr, conf))
    val portsResponse = masterEndpoint.askSync[BoundPortsResponse](BoundPortsRequest)
    (rpcEnv, portsResponse.webUIPort, portsResponse.restPort)
  }
}
