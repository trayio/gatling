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

package io.gatling.decoupled.protocol

import com.softwaremill.quicklens._

import scala.concurrent.duration._

object SqsProtocolBuilder {

  implicit def toSqsProtocol(builder: SqsProtocolBuilder): SqsProtocol = builder.build

  def apply(awsRegion: String, queueUrl: String): SqsProtocolBuilder = {
    SqsProtocolBuilder(
      SqsProtocol.default(awsRegion, queueUrl)
    )
  }
}

final case class SqsProtocolBuilder(protocol: SqsProtocol) {

  def decoupledResponseTimeoutSeconds(seconds: Int): SqsProtocolBuilder = this.modify(_.protocol.decoupledResponseTimeout).setTo(seconds.seconds)

  def processingTimeout(seconds: Int): SqsProtocolBuilder = this.modify(_.protocol.processingTimeout).setTo(seconds.seconds)

  def build: SqsProtocol = protocol

}
