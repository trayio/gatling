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

package io.gatling.decoupled.state

import java.util.UUID

import akka.Done
import akka.pattern.ask
import akka.actor.ActorRefFactory
import akka.util.Timeout
import io.gatling.commons.util.Clock
import io.gatling.core.action.Action
import io.gatling.core.session.Session
import io.gatling.core.stats.StatsEngine
import io.gatling.decoupled.models.{ NormalExecutionPhase, TriggerPhase }
import io.gatling.decoupled.state.PendingRequestsActor.{ DecoupledResponseReceived, RequestTriggered }

import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration.FiniteDuration

class PendingRequestsManager(
    actorRefFactory: ActorRefFactory,
    statsEngine: StatsEngine,
    clock: Clock,
    pendingTimeout: FiniteDuration,
    askTimeout: FiniteDuration
) {
  private implicit val executor: ExecutionContext = actorRefFactory.dispatcher
  private implicit val askTimeoutObject: Timeout = Timeout(askTimeout)

  private val actor = PendingRequestsActor(actorRefFactory, statsEngine, clock, pendingTimeout)

  def registerTrigger(id: UUID, trigger: TriggerPhase, session: Session, next: Action): Future[Done] = {
    (actor ? RequestTriggered(id, trigger, session, next)).map(_ => Done)
  }

  def registerResponse(id: UUID, executionPhases: Seq[NormalExecutionPhase]): Future[Done] = {
    (actor ? DecoupledResponseReceived(id, executionPhases)).map(_ => Done)
  }

}
