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

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters.EnumerationHasAsScala

import io.gatling.core.akka.BaseActor
import io.gatling.core.config.GatlingConfiguration
import io.gatling.datadog.DatadogRequests.sendMetrics

import akka.actor.{ Actor, Props, Stash }
import com.typesafe.scalalogging.Logger
import scaladog.Client
import scaladog.api.{ DatadogSite, StatusResponse }
import scaladog.api.metrics.{ MetricType, Point, Series }

object DatadogRequests {
  def sendTotalStartedUsers(
      client: Client,
      batchSize: Int,
      runId: String,
      simulation: String,
      startedUsers: List[UserLabels]
  ): Seq[StatusResponse] = {
    val requests = for {
      (scenario, users) <- startedUsers.groupBy(_.scenario)
      batchedUsers <- users.grouped(batchSize).toSeq
    } yield {
      sendCount(
        client,
        "gatling.performance_tests.total_started_users",
        batchedUsers.length,
        Seq(
          s"run:$runId",
          s"simulation:$simulation"
        )
      )
    }
    requests.toSeq
  }

  def sendTotalFinishedUsers(
      client: Client,
      batchSize: Int,
      runId: String,
      simulation: String,
      finishedUsers: List[UserLabels]
  ): Seq[StatusResponse] = {
    val requests = for {
      (scenario, users) <- finishedUsers.groupBy(_.scenario)
      batchedUsers <- users.grouped(batchSize).toSeq
    } yield {
      sendCount(
        client,
        "gatling.performance_tests.total_finished_users",
        batchedUsers.length,
        Seq(
          s"run:$runId",
          s"simulation:$simulation"
        )
      )
    }
    requests.toSeq
  }

  def sendTotalErrors(
      client: Client,
      batchSize: Int,
      runId: String,
      simulation: String,
      errors: List[ErrorLabels]
  ): Seq[StatusResponse] = {
    val requests = for {
      batchedErrors <- errors.grouped(batchSize).toSeq
    } yield {
      sendCount(
        client,
        "gatling.performance_tests.total_errors",
        batchedErrors.length,
        Seq(
          s"run:$runId",
          s"simulation:$simulation"
        )
      )
    }
    requests
  }

  def sendRequestLatencyMillis(
      client: Client,
      batchSize: Int,
      runId: String,
      simulation: String,
      requestLatency: List[ResponseLabels]
  ): Seq[StatusResponse] = {
    val requests = for {
      (status, requestsByScenarioAndStatus) <- requestLatency.groupBy(
        _.status
      )
      batchedRequests <- requestsByScenarioAndStatus.grouped(batchSize).toSeq
    } yield {
      sendHistogram(
        client,
        "gatling.performance_tests.request_latency_millis",
        batchedRequests,
        Seq(
          s"run:$runId",
          s"simulation:$simulation",
          s"status$status"
        )
      )
    }
    requests.toSeq
  }

  def sendMetrics(client: Client, batchSize: Int, data: DatadogData)(implicit
      logger: Logger
  ): Unit = {
    val startedUsersIds = data.startedUsers.keys().asScala.toSeq
    val startedUsers =
      startedUsersIds.map(id => data.startedUsers.get(id)).toList
    val startedUsersRequests = sendTotalStartedUsers(
      client,
      batchSize,
      data.runId,
      data.simulation,
      startedUsers
    )
    startedUsersIds.foreach(id => DatadogData.remove(id, data.startedUsers))

    val finishedUsersIds = data.finishedUsers.keys().asScala.toSeq
    val finishedUsers =
      finishedUsersIds.map(id => data.finishedUsers.get(id)).toList
    val finishedUsersRequests = sendTotalFinishedUsers(
      client,
      batchSize,
      data.runId,
      data.simulation,
      finishedUsers
    )
    finishedUsersIds.foreach(id => DatadogData.remove(id, data.finishedUsers))

    val requestLatencyIds = data.requestLatency.keys().asScala.toSeq
    val requestLatency =
      requestLatencyIds.map(id => data.requestLatency.get(id)).toList
    val requestLatencyRequests = sendRequestLatencyMillis(
      client,
      batchSize,
      data.runId,
      data.simulation,
      requestLatency
    )
    requestLatencyIds.foreach(id => DatadogData.remove(id, data.requestLatency))

    val errorCounterIds = data.errorCounter.keys().asScala.toSeq
    val errorCounter =
      errorCounterIds.map(id => data.errorCounter.get(id)).toList
    val errorCounterRequests = sendTotalErrors(
      client,
      batchSize,
      data.runId,
      data.simulation,
      errorCounter
    )
    errorCounterIds.foreach(id => DatadogData.remove(id, data.errorCounter))
  }

  def sendHistogram(
      client: Client,
      metricName: String,
      dataPoints: Seq[DatadogLabels],
      tags: Seq[String]
  ): StatusResponse = {

    val values = dataPoints.map(_.value.doubleValue)

    client.metrics.postSingleMetric(
      metric = s"$metricName.avg",
      metricType = MetricType.Gauge,
      value = values.sum / dataPoints.length,
      tags = tags
    )

    client.metrics.postSingleMetric(
      metric = s"$metricName.median",
      metricType = MetricType.Gauge,
      value = median(values),
      tags = tags
    )

    client.metrics.postSingleMetric(
      metric = s"$metricName.max",
      metricType = MetricType.Gauge,
      value = values.max,
      tags = tags
    )

    client.metrics.postSingleMetric(
      metric = s"$metricName.95percentile",
      metricType = MetricType.Gauge,
      value = p95(values),
      tags = tags
    )
  }

  private def median(values: Seq[Double]): Double = {
    val sortedList = values.sorted
    val mid = values.size / 2
    if (values.size % 2 == 0) {
      (sortedList(mid - 1) + sortedList(mid)) / 2
    } else {
      sortedList(mid)
    }
  }

  private def p95(values: Seq[Double]): Double = {
    // calculate the 95th percentile
    val sortedList = values.sorted
    val index = (sortedList.size * 0.95).toInt - 1
    sortedList(index)
  }

  def sendCount(
      client: Client,
      metricName: String,
      count: BigDecimal,
      tags: Seq[String]
  ): StatusResponse =
    client.metrics.postSingleMetric(
      metric = metricName,
      value = count,
      tags = tags
    )
}

private[datadog] object MetricsSender {
  def props(
      configuration: GatlingConfiguration
  )(implicit ec: ExecutionContext, logger: Logger): Props = {
    val apiKey = configuration.data.datadog.apiKey
    val appKey = configuration.data.datadog.appKey
    val site = configuration.data.datadog.site
    val bufferSize = configuration.data.datadog.bufferSize
    Props(
      new DatadogApiSender(apiKey, appKey, site, bufferSize)
    )
  }
}

private[datadog] class DatadogApiSender(
    apiKey: String,
    appKey: String,
    site: String,
    bufferSize: Int
)(implicit ec: ExecutionContext, logger: Logger)
    extends BaseActor
    with Stash {
  val client = scaladog.Client(
    apiKey,
    appKey,
    DatadogSite.withName(site)
  )

  def receive: Receive = {
    case data: DatadogData =>
      logger.info(
        s"Received request to push data to Datadog for run: ${data.runId}"
      )
      sendMetrics(client, bufferSize, data)
    case _ => logger.info("wtf")
  }
}
