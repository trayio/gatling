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

import java.time.Instant
import java.util.UUID

import akka.Done
import io.gatling.BaseSpec
import io.gatling.decoupled.models.{ ExecutionPhase, NormalExecutionPhase }
import io.gatling.decoupled.state.PendingRequestsState
import org.mockito.Mockito.when
import software.amazon.awssdk.services.sqs.model.Message
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers._
import org.mockito.stubbing.OngoingStubbing
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.Future

class SqsMessageProcessorSpec extends BaseSpec with ScalaFutures {

  behavior of "SqsMessageProcessor"

  it should "parse and forward the SQS message" in new Fixtures {

    val message = givenProperMessage
    givenRegistrationCallsWorks

    val result = processor.apply(message)

    result.futureValue shouldBe Done
    expectResponseRegistration

  }

  it should "fail if message is invalid" in new Fixtures {
    val message = givenInvalidMessage
    givenRegistrationCallsWorks

    val result = processor.apply(message)

    result.failed.futureValue shouldBe a[Exception]
    expectNoResponseRegistration
  }

  it should "fail if state change fails" in new Fixtures {
    val message = givenProperMessage
    givenRegistrationCallsFail

    val result = processor.apply(message)

    result.failed.futureValue shouldBe a[Exception]
  }

  trait Fixtures {

    val state = mock[PendingRequestsState]

    val processor = new SqsMessageProcessor(state)

    val id = UUID.fromString("17d754d8-6c15-4c0c-b13b-b7d6318095b6")
    val phases = Seq(
      NormalExecutionPhase("phase-1", Instant.parse("2020-02-13T09:57:38Z")),
      NormalExecutionPhase("phase-2", Instant.parse("2020-02-13T10:57:38Z"))
    )

    def givenProperMessage: Message = {
      message(
        """
          |{
          |  "id": "17d754d8-6c15-4c0c-b13b-b7d6318095b6",
          |  "phases": [
          |    {
          |      "name": "phase-1",
          |      "time": "2020-02-13T09:57:38Z"
          |    },
          |    {
          |      "name": "phase-2",
          |      "time": "2020-02-13T10:57:38Z"
          |    }
          |  ]
          |}""".stripMargin
      )
    }

    def givenInvalidMessage: Message = {
      message(
        """[{
          | "name": "phase-1"
          | "time": "yesterday" 
          |}]""".stripMargin
      )
    }

    private def message(body: String) = {
      val builder = Message.builder()
      builder.body(body).build()
    }

    def givenRegistrationCallsWorks: Unit = {
      when(state.registerResponse(any[UUID], any[Seq[NormalExecutionPhase]])) thenReturn Future.successful(Done)
    }

    def givenRegistrationCallsFail: Unit = {
      when(state.registerResponse(any[UUID], any[Seq[NormalExecutionPhase]])) thenReturn Future.failed(new Exception("error"))
    }

    def expectResponseRegistration: Unit = {
      verify(state, times(1)).registerResponse(id, phases)
    }

    def expectNoResponseRegistration: Unit = {
      verify(state, never).registerResponse(any[UUID], any[Seq[NormalExecutionPhase]])
    }

  }

}
