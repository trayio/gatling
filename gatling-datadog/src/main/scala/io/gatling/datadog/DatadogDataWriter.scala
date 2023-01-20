/*
 * Copyright 2011-2023 GatlingCorp (https://gatling.io)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.gatling.datadog

import java.time.Instant
import java.util.UUID

import scala.concurrent.{ blocking, ExecutionContext, Future }
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters.{ ConcurrentMapHasAsScala, EnumerationHasAsScala }
import scala.util.{ Failure, Success }

import io.gatling.commons.util.Clock
import io.gatling.core.config.GatlingConfiguration
import io.gatling.core.stats.writer.{ DataWriter, DataWriterMessage }
import io.gatling.core.stats.writer.DataWriterMessage.LoadEvent
import io.gatling.core.stats.writer.DataWriterMessage.LoadEvent.{ Error => EventError, Response, UserEnd, UserStart }
import io.gatling.core.util.NameGen
import io.gatling.datadog.DatadogRequests.{ sendMetrics, sendRequestLatencyMillis, sendTotalErrors, sendTotalFinishedUsers, sendTotalStartedUsers }

import akka.actor.{ Actor, ActorRef, Props }
import com.typesafe.scalalogging.Logger
import scaladog.Client
import scaladog.api.{ DatadogSite, StatusResponse }
import scaladog.api.metrics.{ MetricType, Point, Series }

private[gatling] class DatadogDataWriter(
                                          clock: Clock,
                                          configuration: GatlingConfiguration
                                        ) extends DataWriter[DatadogData]
  with NameGen {

  /**
   * Datadog recommend instantiating a client for every thread writing to the API
   */
  private def buildClient: Client =
    scaladog.Client(
      configuration.data.datadog.apiKey,
      configuration.data.datadog.appKey,
      DatadogSite.withName(configuration.data.datadog.site)
    )

  private val flushTimerName = "DatadogFlushTimer"

  override def onInit(init: DataWriterMessage.Init): DatadogData = {
    logger.info(
      s"Initializing Datadog DataWriter for run: ${init.runMessage.runId}"
    )
    val metricsSender: ActorRef = context.actorOf(
      MetricsSender.props(configuration, dispatcher, logger),
      genName("metricsSender")
    )
    startTimerAtFixedRate(flushTimerName, DataWriterMessage.Flush, configuration.data.datadog.flushRate)
    DatadogData.initialise(metricsSender, init)
  }

  override def onFlush(data: DatadogData): Unit = {
    logger.info(s"Flushing to Datadog for run ${data.runId}")
    sendMetricsToDatadog(data)

  }

  override def onCrash(cause: String, data: DatadogData): Unit =
    logger.error(cause)

  override def onStop(data: DatadogData): Unit = {
    cancelTimer(flushTimerName)
    onFlush(data)
  }

  override def onMessage(message: LoadEvent, data: DatadogData): Unit =
    message match {
      case userStart: UserStart => onUserStartMessage(userStart, data)
      case userEnd: UserEnd     => onUserEndMessage(userEnd, data)
      case response: Response   => onResponseMessage(response, data)
      case error: EventError    => onErrorMessage(error, data)
      case _                    => ()
    }

  private def onUserStartMessage(user: UserStart, data: DatadogData): Unit =
    data.startedUsers.put(
      UUID.randomUUID(),
      UserLabels(currentInstant, user.scenario, BigDecimal.valueOf(1))
    )

  private def onUserEndMessage(user: UserEnd, data: DatadogData): Unit =
    data.finishedUsers.put(
      UUID.randomUUID(),
      UserLabels(currentInstant, user.scenario, BigDecimal.valueOf(1))
    )

  private def onResponseMessage(response: Response, data: DatadogData): Unit =
    data.requestLatency.put(
      UUID.randomUUID(),
      ResponseLabels(
        currentInstant,
        response.scenario,
        response.status.toString,
        BigDecimal.valueOf(
          response.endTimestamp - response.startTimestamp
        )
      )
    )

  private def onErrorMessage(error: EventError, data: DatadogData): Unit =
    data.errorCounter.put(
      UUID.randomUUID(),
      ErrorLabels(currentInstant, BigDecimal.valueOf(1))
    )

  private def currentInstant = Instant.ofEpochMilli(clock.nowMillis)

  private def sendMetricsToDatadog(data: DatadogData): Unit = {
    logger.info(
      s"Preparing to dispatch to Datadog MetricsSender Actor for run: ${data.runId}"
    )
    data.metricsSender ! data
  }
}

private[datadog] object MetricsSender {
  def props(
             configuration: GatlingConfiguration,
             dispatcher: ExecutionContext,
             logger: Logger
           ): Props = {
    val apiKey = configuration.data.datadog.apiKey
    val appKey = configuration.data.datadog.appKey
    val site = configuration.data.datadog.site
    Props(new DatadogApiSender(apiKey, appKey, site, dispatcher, logger))
  }
}

private[datadog] class DatadogApiSender(
                                         apiKey: String,
                                         appKey: String,
                                         site: String,
                                         dispatcher: ExecutionContext,
                                         logger: Logger
                                       ) extends Actor {
  val client = scaladog.Client(
    apiKey,
    appKey,
    DatadogSite.withName(site)
  )

  def receive: Receive = { case data: DatadogData =>
    logger.info(
      s"Received request to push data to Datadog for run: ${data.runId}"
    )
    sendMetrics(client, data)(dispatcher, logger)
  }
}

object DatadogRequests {
  def sendTotalStartedUsers(
                             client: Client,
                             runId: String,
                             simulation: String,
                             startedUsers: List[(UUID, UserLabels)]
                           )(implicit
                             ec: ExecutionContext
                           ): Seq[Future[StatusResponse]] = {
    val futures = for {
      (scenario, users) <- startedUsers.groupBy(_._2.scenario)
      batchedUsers <- users.grouped(500).toSeq
    } yield {
      send(
        client,
        "gatling.performance_tests.total_started_users",
        MetricType.Gauge,
        batchedUsers,
        Seq(
          s"run:$runId",
          s"simulation:$simulation",
          s"scenario:$scenario"
        )
      )
    }
    futures.toSeq
  }

  def sendTotalFinishedUsers(
                              client: Client,
                              runId: String,
                              simulation: String,
                              finishedUsers: List[(UUID, UserLabels)]
                            )(implicit
                              ec: ExecutionContext
                            ): Seq[Future[StatusResponse]] = {
    val futures = for {
      (scenario, users) <- finishedUsers.groupBy(_._2.scenario)
      batchedUsers <- users.grouped(500).toSeq
    } yield {
      send(
        client,
        "gatling.performance_tests.total_finished_users",
        MetricType.Gauge,
        batchedUsers,
        Seq(
          s"run:$runId",
          s"simulation:$simulation",
          s"scenario:$scenario"
        )
      )
    }
    futures.toSeq
  }

  def sendTotalErrors(
                       client: Client,
                       runId: String,
                       simulation: String,
                       errors: List[(UUID, ErrorLabels)]
                     )(implicit ec: ExecutionContext): Seq[Future[StatusResponse]] = {
    val futures = for {
      batchedErrors <- errors.grouped(500).toSeq
    } yield {
      send(
        client,
        "gatling.performance_tests.total_errors",
        MetricType.Gauge,
        batchedErrors,
        Seq(
          s"run:$runId",
          s"simulation:$simulation"
        )
      )
    }
    futures
  }

  def sendRequestLatencyMillis(
                                client: Client,
                                runId: String,
                                simulation: String,
                                requestLatency: List[(UUID, ResponseLabels)]
                              )(implicit
                                ec: ExecutionContext
                              ): Seq[Future[StatusResponse]] = {
    val futures = for {
      (scenario, requestsByScenario) <- requestLatency.groupBy(_._2.scenario)
      (status, requestsByScenarioAndStatus) <- requestsByScenario.groupBy(_._2.status)
      batchedRequests <- requestsByScenarioAndStatus.grouped(500).toSeq
    } yield {
      send(
        client,
        "gatling.performance_tests.request_latency_millis",
        MetricType.Gauge,
        batchedRequests,
        Seq(
          s"run:$runId",
          s"simulation:$simulation",
          s"scenario:$scenario",
          s"status$status"
        )
      )
    }
    futures.toSeq
  }

  def sendMetrics(client: Client, data: DatadogData)(implicit
                                                     ec: ExecutionContext,
                                                     logger: Logger
  ): Unit = {
    logger.info(s"sendMetrics executing for run: ${data.runId}")
    val startedUsers = data.startedUsers
    val startedUsersRequests = sendTotalStartedUsers(
      client,
      data.runId,
      data.simulation,
      startedUsers.asScala.toList
    )

    val finishedUsers = data.finishedUsers
    val finishedUsersRequests = sendTotalFinishedUsers(
      client,
      data.runId,
      data.simulation,
      finishedUsers.asScala.toList
    )

    val requestLatency = data.requestLatency
    val requestLatencyRequests = sendRequestLatencyMillis(
      client,
      data.runId,
      data.simulation,
      requestLatency.asScala.toList
    )

    val errorCounter = data.errorCounter
    val errorCounterRequests = sendTotalErrors(
      client,
      data.runId,
      data.simulation,
      errorCounter.asScala.toList
    )

    startedUsersRequests.foreach { startedUsersRequest =>
      startedUsersRequest.onComplete {
        case Success(StatusResponse(status)) =>
          logger.info(s"Started users request completed with status $status")
          startedUsers
            .keys()
            .asScala
            .toSeq
            .foreach(id => DatadogData.remove(id, startedUsers))
        case Failure(e) =>
          logger.error(s"Started users request failed with error $e")
      }
    }

    finishedUsersRequests.foreach { finishedUsersRequest =>
      finishedUsersRequest.onComplete {
        case Success(StatusResponse(status)) =>
          logger.info(s"Finished users request completed with status $status")
          finishedUsers
            .keys()
            .asScala
            .toSeq
            .foreach(id => DatadogData.remove(id, finishedUsers))
        case Failure(e) =>
          logger.error(s"Finished users request failed with error $e")
      }
    }

    requestLatencyRequests.foreach { requestLatencyRequest =>
      requestLatencyRequest.onComplete {
        case Success(StatusResponse(status)) =>
          logger.info(s"Request latency request completed with status $status")
          requestLatency
            .keys()
            .asScala
            .toSeq
            .foreach(id => DatadogData.remove(id, requestLatency))
        case Failure(e) =>
          logger.error(s"Request latency request failed with error $e")
      }
    }

    errorCounterRequests.foreach { errorCounterRequest =>
      errorCounterRequest.onComplete {
        case Success(StatusResponse(status)) =>
          logger.info(s"Error count request completed with status $status")
          errorCounter
            .keys()
            .asScala
            .toSeq
            .foreach(id => DatadogData.remove(id, errorCounter))
        case Failure(e) =>
          logger.error(s"Error count request failed with error $e")
      }
    }
  }

  def send(
            client: Client,
            metricName: String,
            metricType: MetricType,
            dataPoints: Seq[(UUID, DatadogLabels)],
            tags: Seq[String]
          )(implicit ec: ExecutionContext): Future[StatusResponse] =
    Future {
      blocking {
        client.metrics
          .postMetrics(
            Seq(
              Series(
                metric = metricName,
                points = dataPoints.map { case (_, labels: DatadogLabels) =>
                  Point(labels.instant, labels.value)
                },
                tags = tags,
                metricType = metricType
              )
            )
          )
      }
    }
}