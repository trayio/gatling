/*
 * Copyright 2011-2022 GatlingCorp (https://gatling.io)
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

import scala.concurrent.{ExecutionContext, Future, blocking}
import scala.jdk.CollectionConverters.{
  ConcurrentMapHasAsScala,
  EnumerationHasAsScala
}
import scala.util.{Failure, Success}

import io.gatling.commons.util.Clock
import io.gatling.core.config.GatlingConfiguration
import io.gatling.core.stats.writer.{DataWriter, DataWriterMessage}
import io.gatling.core.stats.writer.DataWriterMessage.LoadEvent
import io.gatling.core.stats.writer.DataWriterMessage.LoadEvent.{
  Response,
  UserEnd,
  UserStart,
  Error => EventError
}
import io.gatling.core.util.NameGen
import io.gatling.datadog.DatadogRequests.{
  sendRequestLatencySeconds,
  sendTotalErrors,
  sendTotalFinishedUsers,
  sendTotalStartedUsers
}
import scaladog.Client
import scaladog.api.{DatadogSite, StatusResponse}
import scaladog.api.metrics.{MetricType, Point, Series}

private[gatling] class DatadogDataWriter(
    clock: Clock,
    configuration: GatlingConfiguration
) extends DataWriter[DatadogData]
    with NameGen {

  /** Datadog recommend instantiating a client for every thread writing to the API
    */
  private def buildClient: Client =
    scaladog.Client(
      configuration.data.datadog.apiKey,
      configuration.data.datadog.appKey,
      DatadogSite.withName(configuration.data.datadog.site)
    )

  override def onInit(init: DataWriterMessage.Init): DatadogData = {
    logger.info(s"Initialising Datadog DataWriter for run: ${init.runMessage}")
    DatadogData.initialise(init)
  }

  override def onFlush(data: DatadogData): Unit = {
    logger.info(s"Flushing to Datadog")

    val startedUsers = data.startedUsers
    val startedUsersRequests = sendTotalStartedUsers(
      buildClient,
      data.simulation,
      startedUsers.asScala.toList
    )

    val finishedUsers = data.finishedUsers
    val finishedUsersRequests = sendTotalFinishedUsers(
      buildClient,
      data.simulation,
      finishedUsers.asScala.toList
    )

    val requestLatency = data.requestLatency
    val requestLatencyRequests = sendRequestLatencySeconds(
      buildClient,
      data.simulation,
      requestLatency.asScala.toList
    )

    val errorCounter = data.errorCounter
    val errorCounterRequests = sendTotalErrors(
      buildClient,
      data.simulation,
      errorCounter.asScala.toList
    )

    startedUsersRequests.foreach { startedUsersRequest =>
      startedUsersRequest.onComplete {
        case Success(StatusResponse(status)) =>
          logger.info(
            s"Flushed started users to Datadog with response: $status"
          )
          startedUsers
            .keys()
            .asScala
            .toSeq
            .foreach(id => DatadogData.remove(id, startedUsers))
        case Failure(e) =>
          logger.error(s"Failed to send started users to Datadog", e)
      }
    }

    finishedUsersRequests.foreach { finishedUsersRequest =>
      finishedUsersRequest.onComplete {
        case Success(StatusResponse(status)) =>
          logger.info(
            s"Flushed finished users to Datadog with response: $status"
          )
          finishedUsers
            .keys()
            .asScala
            .toSeq
            .foreach(id => DatadogData.remove(id, finishedUsers))
        case Failure(e) =>
          logger.error(s"Failed to send finished users to Datadog", e)
      }
    }

    requestLatencyRequests.foreach { requestLatencyRequest =>
      requestLatencyRequest.onComplete {
        case Success(StatusResponse(status)) =>
          logger.info(
            s"Flushed request latency to Datadog with response: $status"
          )
          requestLatency
            .keys()
            .asScala
            .toSeq
            .foreach(id => DatadogData.remove(id, requestLatency))
        case Failure(e) =>
          logger.error(s"Failed to send request latency to Datadog", e)
      }
    }

    errorCounterRequests.foreach { errorCounterRequest =>
      errorCounterRequest.onComplete {
        case Success(StatusResponse(status)) =>
          logger.info(
            s"Flushed error counter to Datadog with response: $status"
          )
          errorCounter
            .keys()
            .asScala
            .toSeq
            .foreach(id => DatadogData.remove(id, errorCounter))
        case Failure(e) =>
          logger.error(s"Failed to send error counter to Datadog", e)
      }
    }

  }

  override def onCrash(cause: String, data: DatadogData): Unit =
    logger.error(cause)

  override def onStop(data: DatadogData): Unit = {
    logger.info(s"Received Stop message")
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

  private def onUserStartMessage(user: UserStart, data: DatadogData): Unit = {
    logger.info(s"Received UserStart message")
    data.startedUsers.put(
      UUID.randomUUID(),
      UserLabels(currentInstant, user.scenario, BigDecimal.valueOf(1))
    )
    if (data.startedUsers.size() >= configuration.data.datadog.flushSize) {
      onFlush(data)
    }
  }

  private def onUserEndMessage(user: UserEnd, data: DatadogData): Unit = {
    logger.info(s"Received UserEnd message")
    data.finishedUsers.put(
      UUID.randomUUID(),
      UserLabels(currentInstant, user.scenario, BigDecimal.valueOf(1))
    )
    if (data.finishedUsers.size() >= configuration.data.datadog.flushSize) {
      onFlush(data)
    }
  }

  private def onResponseMessage(response: Response, data: DatadogData): Unit = {
    logger.info(s"Received Response message")
    data.requestLatency.put(
      UUID.randomUUID(),
      ResponseLabels(
        currentInstant,
        response.scenario,
        response.status.toString,
        BigDecimal.valueOf(
          (response.endTimestamp - response.startTimestamp) / 1000d
        )
      )
    )
    if (data.requestLatency.size() >= configuration.data.datadog.flushSize) {
      onFlush(data)
    }
  }

  private def onErrorMessage(error: EventError, data: DatadogData): Unit = {
    logger.info(s"Received EventError message")
    data.errorCounter.put(
      UUID.randomUUID(),
      ErrorLabels(currentInstant, BigDecimal.valueOf(1))
    )
    if (data.errorCounter.size() >= configuration.data.datadog.flushSize) {
      onFlush(data)
    }
  }

  private def currentInstant = Instant.ofEpochMilli(clock.nowMillis)
}

object DatadogRequests {
  def sendTotalStartedUsers(
      client: Client,
      simulation: String,
      startedUsers: List[(UUID, UserLabels)]
  )(implicit
      ec: ExecutionContext
  ): Seq[Future[StatusResponse]] = {
    val futures = for {
      (scenario, users) <- startedUsers.groupBy(_._2.scenario)
    } yield {
      send(
        client,
        "total_started_users",
        MetricType.Count,
        users,
        Seq(
          s"simulation:$simulation",
          s"scenario:$scenario"
        )
      )
    }
    futures.toSeq
  }

  def sendTotalFinishedUsers(
      client: Client,
      simulation: String,
      finishedUsers: List[(UUID, UserLabels)]
  )(implicit
      ec: ExecutionContext
  ): Seq[Future[StatusResponse]] = {
    val futures = for {
      (scenario, users) <- finishedUsers.groupBy(_._2.scenario)
    } yield {
      send(
        client,
        "total_finished_users",
        MetricType.Count,
        users,
        Seq(
          s"simulation:$simulation",
          s"scenario:$scenario"
        )
      )
    }
    futures.toSeq
  }

  def sendTotalErrors(
      client: Client,
      simulation: String,
      errors: List[(UUID, ErrorLabels)]
  )(implicit ec: ExecutionContext): Seq[Future[StatusResponse]] =
    Seq(
      send(
        client,
        "total_errors",
        MetricType.Count,
        errors,
        Seq(
          s"simulation:$simulation"
        )
      )
    )

  def sendRequestLatencySeconds(
      client: Client,
      simulation: String,
      requestLatency: List[(UUID, ResponseLabels)]
  )(implicit
      ec: ExecutionContext
  ): Seq[Future[StatusResponse]] = {
    val futures = for {
      (scenario, requestsByScenario) <- requestLatency.groupBy(_._2.scenario)
      (status, requestsByScenarioAndStatus) <- requestsByScenario.groupBy(
        _._2.status
      )
    } yield {
      send(
        client,
        "request_latency_seconds",
        MetricType.Gauge,
        requestsByScenarioAndStatus,
        Seq(
          s"simulation:$simulation",
          s"scenario:$scenario",
          s"status$status"
        )
      )
    }
    futures.toSeq
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
                points =
                  dataPoints.map { case (_, labels: DatadogLabels) =>
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
