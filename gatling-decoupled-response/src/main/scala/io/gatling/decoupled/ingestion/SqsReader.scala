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

import akka.Done
import akka.actor.ActorSystem
import akka.stream.alpakka.sqs.{ MessageAction, SqsSourceSettings }
import akka.stream.alpakka.sqs.scaladsl.{ SqsAckSink, SqsSource }
import akka.stream.scaladsl.Flow
import akka.stream.{ ActorAttributes, Supervision }
import com.github.matsluni.akkahttpspi.AkkaHttpClient
import io.gatling.decoupled.ingestion.SqsReader.{ AwsKeys, MessageProcessor }
import software.amazon.awssdk.auth.credentials.{ AwsBasicCredentials, StaticCredentialsProvider }
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.sqs.model.Message

import scala.concurrent.Future

object SqsReader {
  type MessageProcessor = Message => Future[Done]
  final case class AwsKeys(accessKeyId: String, secretAccessKey: String)

  val alwaysResumeDecider: Supervision.Decider = {
    case _ => Supervision.Resume
  }
}

class SqsReader(processor: MessageProcessor, awsRegion: Region, queueUrl: String, awsKeys: Option[AwsKeys])(implicit actorSystem: ActorSystem) {
  import SqsReader._

  private implicit val awsSqsClient = buildAwsClient
  registerTerminationHook

  def run: Unit = {
    SqsSource(queueUrl, SqsSourceSettings())
      .to(messageProcessing)
      .withAttributes(ActorAttributes.supervisionStrategy(alwaysResumeDecider))
      .run()
  }

  private def buildAwsClient = {
    val awsSqsClientBuilder = SqsAsyncClient
      .builder()
      .region(awsRegion)
      .httpClient(AkkaHttpClient.builder().withActorSystem(actorSystem).build())

    awsKeys.foreach(
      keys => awsSqsClientBuilder.credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(keys.accessKeyId, keys.secretAccessKey)))
    )

    awsSqsClientBuilder.build()
  }

  private def registerTerminationHook = {
    actorSystem.registerOnTermination(awsSqsClient.close())
  }

  private def messageProcessing = {
    Flow[Message]
      .mapAsync(10) { message =>
        processor(message).map(_ => message)(actorSystem.dispatcher)
      }
      .map(MessageAction.Delete(_))
      .to(SqsAckSink(queueUrl))

  }

}
