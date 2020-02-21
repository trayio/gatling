/*
 * Copyright 2011-2020 GatlingCorp (https://gatling.io)
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

package io.gatling.decoupled.ingestion

import java.util.UUID

import akka.Done
import io.gatling.decoupled.ingestion.SqsReader.MessageProcessor
import io.gatling.decoupled.state.PendingRequestsState
import software.amazon.awssdk.services.sqs.model.Message
import io.circe.generic.auto._
import io.circe.parser._
import io.gatling.decoupled.models.NormalExecutionPhase

import scala.concurrent.Future
import scala.util.control.NoStackTrace

class SqsMessageProcessor(pendingRequests: PendingRequestsState) extends MessageProcessor {
  import SqsMessageProcessor._

  override def apply(message: Message): Future[Done] = {
    parseMessage(message.body) match {
      case Right(message) => pendingRequests.registerResponse(message.id, message.phases)
      case Left(error)    => Future.failed(error)
    }
  }

  private def parseMessage(messageBody: String) = {
    for {
      body <- Either.cond(messageBody != null, messageBody, missingBody)
      json <- parse(body)
      sqsMessage <- json.as[SqsMessage]
    } yield sqsMessage
  }
}

object SqsMessageProcessor {
  final case class SqsMessage(id: UUID, phases: Seq[NormalExecutionPhase])
  private val missingBody = new Exception("Message body is missing") with NoStackTrace
}
