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

package io.gatling.decoupled.action

import com.softwaremill.quicklens._
import io.gatling.core.ValidationImplicits
import io.gatling.core.action.Action
import io.gatling.core.session.Expression
import io.gatling.core.structure.ScenarioContext
import io.gatling.decoupled.models.ExecutionId
import io.gatling.decoupled.models.ExecutionId.ExecutionId
import io.gatling.decoupled.protocol.SqsActionBuilder
import io.gatling.http.request.builder.HttpRequestBuilder

final case class DecoupledResponseActionBuilder(name: String, httpRequestBuilder: HttpRequestBuilder, attributes: DecoupledResponseActionAttributes)
    extends SqsActionBuilder
    with ValidationImplicits {

  override def build(ctx: ScenarioContext, next: Action): Action = {
    val executionId: Expression[ExecutionId] = session => ExecutionId(s"$name-${session.userId}")

    val sqsComponents = lookUpSqsComponents(ctx.protocolComponentsRegistry)

    val waitResponse = new WaitDecoupledResponseAction(name, sqsComponents.pendingRequests, next, executionId, ctx)

    val httpRequest = httpRequestBuilder
      .header(attributes.correlationIdHeader, executionId)
      .build(ctx, waitResponse)

    httpRequest
  }

  def correlationIdHeaderName(name: String): DecoupledResponseActionBuilder = this.modify(_.attributes.correlationIdHeader).setTo(name)

}

object DecoupledResponseActionAttributes {
  val correlationIdHeader = "X-GATLING-CORRELATION"

  val Empty: DecoupledResponseActionAttributes = DecoupledResponseActionAttributes(
    correlationIdHeader = correlationIdHeader
  )

}

final case class DecoupledResponseActionAttributes(
    correlationIdHeader: String
)
