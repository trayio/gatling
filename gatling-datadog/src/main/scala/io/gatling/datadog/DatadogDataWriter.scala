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

import io.gatling.commons.util.Clock
import io.gatling.core.config.GatlingConfiguration
import io.gatling.core.stats.writer.{ DataWriter, DataWriterMessage }
import io.gatling.core.stats.writer.DataWriterMessage.LoadEvent
import io.gatling.core.stats.writer.DataWriterMessage.LoadEvent.{ Error => EventError, Response, UserEnd, UserStart }
import io.gatling.core.util.NameGen

import akka.actor.ActorRef

private[gatling] class DatadogDataWriter(
    clock: Clock,
    configuration: GatlingConfiguration
) extends DataWriter[DatadogData]
    with NameGen {

  override def onInit(init: DataWriterMessage.Init): DatadogData = {
    val metricsSender: ActorRef = context.actorOf(
      MetricsSender.props(configuration)(dispatcher, logger),
      genName("metricsSender")
    )
    startTimerAtFixedRate("DatadogDataWriter", DataWriterMessage.Flush, configuration.data.datadog.writePeriod)
    DatadogData.initialise(metricsSender, init)
  }

  override def onFlush(data: DatadogData): Unit =
    sendMetricsToDatadog(data)

  override def onCrash(cause: String, data: DatadogData): Unit = {
    onFlush(data)
    logger.error(cause)
  }

  override def onStop(data: DatadogData): Unit = ()

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

  private def sendMetricsToDatadog(data: DatadogData): Unit =
    data.metricsSender ! data
}
