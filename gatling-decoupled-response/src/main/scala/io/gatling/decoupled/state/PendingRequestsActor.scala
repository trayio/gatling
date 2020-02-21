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

import akka.actor.{ Actor, ActorLogging, ActorRef, ActorRefFactory, Props, Timers }
import io.gatling.commons.stats.{ KO, OK }
import io.gatling.commons.util.Clock
import io.gatling.core.action.Action
import io.gatling.core.session.Session
import io.gatling.core.stats.StatsEngine
import io.gatling.decoupled.models.ExecutionPhase

import scala.concurrent.duration.FiniteDuration

object PendingRequestsActor {

  private[state] def apply(actorRefFactory: ActorRefFactory, statsEngine: StatsEngine, clock: Clock, pendingTimeout: FiniteDuration): ActorRef = {
    actorRefFactory.actorOf(
      Props(new PendingRequestsActor(statsEngine, clock, pendingTimeout))
    )
  }

  final case class ActorState(
      waitingResponse: Map[UUID, RequestTriggered],
      waitingTrigger: Map[UUID, DecoupledResponseReceived]
  ) {
    def withPendingResponse(response: DecoupledResponseReceived): ActorState = {
      copy(waitingTrigger = waitingTrigger + (response.id -> response))
    }

    def withPendingTrigger(trigger: RequestTriggered): ActorState = {
      copy(waitingResponse = waitingResponse + (trigger.id -> trigger))
    }

    def filterManagedId(id: UUID): ActorState = {
      copy(waitingResponse = waitingResponse - id, waitingTrigger = waitingTrigger - id)
    }

  }

  object ActorState {
    val empty: ActorState = ActorState(Map.empty, Map.empty)
  }

  sealed trait ActorMessage
  final case class RequestTriggered(id: UUID, initialPhase: ExecutionPhase, session: Session, next: Action) extends ActorMessage
  final case class DecoupledResponseReceived(id: UUID, executionPhases: Seq[ExecutionPhase]) extends ActorMessage
  final case class WaitResponseTimeout(id: UUID) extends ActorMessage
  final case class WaitTriggerTimeout(id: UUID) extends ActorMessage

  sealed trait ActorResponse
  final case object MessageAck extends ActorResponse

  def genName(id: UUID, from: String, to: String): String = {
    s"$id: $from -> $to"
  }
  val errorName = "Error"
  def genErrorName(id: UUID): String = {
    s"$id: $errorName"
  }
}

private[state] class PendingRequestsActor(statsEngine: StatsEngine, clock: Clock, pendingTimeout: FiniteDuration) extends Actor with ActorLogging with Timers {
  import PendingRequestsActor._

  override def receive: Receive = receiveWithState(ActorState.empty)

  private def receiveWithState(state: ActorState): Receive = {

    case trigger @ RequestTriggered(id, _, _, _) if state.waitingResponse.contains(id) =>
      log.debug("Duplicate trigger received: {}", trigger)
      onWrongMessageReceived(id, state)
      ackMessage

    case trigger @ RequestTriggered(id, _, _, _) if state.waitingTrigger.contains(id) =>
      val response = state.waitingTrigger(id)
      onTriggerAndResponseAvailable(trigger, response, state)
      ackMessage

    case trigger: RequestTriggered =>
      onOnlyTriggerAvailable(trigger, state)
      ackMessage

    case response @ DecoupledResponseReceived(_, phases) if phases.isEmpty =>
      log.debug("Response with no phases received: {}", response)
      onWrongMessageReceived(response.id, state)
      ackMessage

    case response @ DecoupledResponseReceived(_, phases) if hasDuplicatedPhaseNames(phases) =>
      log.debug("Response with duplicate phase names received: {}", response)
      onWrongMessageReceived(response.id, state)
      ackMessage

    case response @ DecoupledResponseReceived(id, _) if state.waitingResponse.contains(id) =>
      val trigger = state.waitingResponse(id)
      onTriggerAndResponseAvailable(trigger, response, state)
      ackMessage

    case response: DecoupledResponseReceived =>
      onOnlyResponseAvailable(response, state)
      ackMessage

    case WaitResponseTimeout(id) =>
      log.debug("Response not received: {}", id)
      onWrongMessageReceived(id, state)
      ackMessage

    case WaitTriggerTimeout(id) =>
      log.debug("Trigger not received: {}", id)
      onWrongMessageReceived(id, state)
      ackMessage

  }

  private def onOnlyTriggerAvailable(trigger: RequestTriggered, state: ActorState): Unit = {
    timers.startSingleTimer(trigger.id, WaitResponseTimeout(trigger.id), pendingTimeout)

    context.become(
      receiveWithState(state.withPendingTrigger(trigger)),
      true
    )
  }

  private def onOnlyResponseAvailable(response: DecoupledResponseReceived, state: ActorState): Unit = {
    timers.startSingleTimer(response.id, WaitTriggerTimeout(response.id), pendingTimeout)

    context.become(
      receiveWithState(state.withPendingResponse(response)),
      true
    )
  }

  private def hasDuplicatedPhaseNames(phases: Seq[ExecutionPhase]): Boolean = {
    val allNames = phases.map(_.name) :+ ExecutionPhase.triggerPhaseName
    allNames.diff(allNames.distinct).nonEmpty
  }

  private def onTriggerAndResponseAvailable(trigger: RequestTriggered, response: DecoupledResponseReceived, state: ActorState) = {
    timers.cancel(trigger.id)

    val allPhases = (trigger.initialPhase +: response.executionPhases).toList
    if (phaseTimesAreSequential(allPhases)) {
      onValidTriggerAndResponseAvailable(trigger, allPhases, state)

    } else {
      log.debug("Non sequential times received: {}", allPhases)
      onWrongMessageReceived(response.id, state)
    }

  }

  private def phaseTimesAreSequential(phases: List[ExecutionPhase]) = {

    phases.sliding(2).forall {
      case from :: to :: Nil => from.time.isBefore(to.time)
      case _                 => false
    }
  }

  private def onValidTriggerAndResponseAvailable(trigger: RequestTriggered, allPhases: List[ExecutionPhase], state: ActorState) = {
    val id = trigger.id

    allPhases.sliding(2).foreach {
      case from :: to :: Nil =>
        logPhaseTransition(id, trigger.session, from, to)
      case _ => ()
    }

    trigger.next ! trigger.session

    context.become(
      receiveWithState(state.filterManagedId(id)),
      true
    )

  }

  private def onWrongMessageReceived(id: UUID, state: ActorState) = {
    state.waitingResponse.get(id).foreach { trigger =>
      logFailure(id, trigger.session, trigger.initialPhase)
    }

    context.become(
      receiveWithState(state.filterManagedId(id)),
      true
    )
  }

  private def logPhaseTransition(
      id: UUID,
      session: Session,
      from: ExecutionPhase,
      to: ExecutionPhase
  ) = {
    statsEngine.logResponse(
      session,
      genName(id, from.name, to.name),
      from.time.toEpochMilli,
      to.time.toEpochMilli,
      OK,
      None,
      None
    )
  }

  private def logFailure(id: UUID, session: Session, from: ExecutionPhase) = {
    statsEngine.logResponse(
      session,
      genErrorName(id),
      from.time.toEpochMilli,
      clock.nowMillis,
      KO,
      None,
      None
    )
  }

  private def ackMessage = {
    sender() ! MessageAck
  }

}
