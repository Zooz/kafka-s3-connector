# kafka-s3-connector (Kafka to S3 Connector)

`kafka-s3-connector` is an easy to use web service that allows reading data from Kafka, transforming it and writing the results out to S3. It supports the following features:

* __Exactly-once guarantee:__ Data read from Kafka will be written once, and only once, to S3.
* __Automatically scalable:__ Assuming your Kafka topic has multiple partitions, you can run multiple instances of `kafka-s3-connector` to evenly divide the workload. Adding more instances on runtime will trigger a transparent rebalance of work between all running consumers.
* __Easy to use:__ `kafka-s3-connector` is a not a library, but a complete web service. Just set a Kafka topic as input and a target S3 bucket to get started immediately. It comes out-of-the box with a simple UI and exposes **Prometheus** metrics and a healthcheck/liveness endpoint for an easy deployment to **DC/OS** or **Kubernetes**.  
* __Completely customizable__: Written in Scala, `kafka-s3-connector` makes it it easy to write your own data transformations (called `mappers`) and health checks. Almost everything else can be modified by setting and tweaking the service's environment variables.

## Alternatives to kafka-s3-connector

* __Stream-processing engines (Spark / Flink / Samza / Storm / ...)__ - Most popular stream-processing engines have good integration with Kafka and S3. However - they require some expertise to properly install and manage.
* __Writing your own Service: Kafka Streams + Kafka Connect__ - *Kafka Streams* can be used to read data from one Kafka topic, transform it and write it out to another topic. *Kafka Connect* can then be used to publish this topic to S3. Both tools provide exactly-once guarantee.
* __Writing your own Service: Akka Streams / Alpakka__ - *Alpakka* (itself based on *Akka Streams*) can be used to read, transform and write the data directly from Kafka to S3. Unlike *Kafka Streams*, there is no need for a staging topic to contain the transformed data. However, *Alapakka* doesn't have an exactly-once guarantee (at least - as far as we're aware of).

## Quickstart Guide
 
### Build and run Locally

Start by [installing sbt](https://www.scala-sbt.org/1.0/docs/Setup.html).

Clone the project to your local machine

```sh
git clone https://github.com/Zooz/kafka-s3-connector.git

cd kafka-s3-connector
```

Build a project *zip* file

```sh
sbt dist
```

Unzip the binary files to your home directory

```sh
unzip -d ~/ ./target/universal/kafkas3connector*.zip
```

Setup your kafka and s3 details and launch the service
```sh
export KAFKA_HOST=kafka-server1:9092,kafka-server2:9092,kafka-server3:9092
export SOURCE_KAFKA_TOPIC=my-topic
export S3_BUCKET_NAME=my-bucket
export S3_ACCESS_KEY=ABCDEFGHIJKLMNOPQRST
export S3_SECRET_KEY=1234567890123456789/1234/123456789012345
```

_(__Note__: Setting S3 Key and Secret values are optional. If not set - `kafka-s3-connector` will assume an [Instance Profile authentication](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_use_switch-role-ec2_instance-profiles.html), more common for environments running on EC2)_

Launch the `kafka-s3-connector`
```
cd ~/kafkas3connector-*/
./bin/kafkas3connector
```

### Service Endpoints
Point your browser to one of the following endpoints to track the status of your service:
* https://localhost:9000/  (Service's summary page)
* http://localhost:9000/health   (health/liveliness endpoint. For integration with _DC/OS_ or _Kubernetes_)
* http://localhost:9000/metrics (_Prometheus_ integration endpoint)

### Build your own Docker

You can easily build your own docker by cloning the project to your local machine and then use the `build-image.sh` script to build and publish it.

```sh
# Replace this with the full docker path
export APP_IMAGE=your.docker-registry.com:1234/kafka-s3-connector/master:my-tag

cd ~/kafka-s3-connector/

./scripts/build-image.sh
```

## Customizing kafka-s3-connector

### Configuration Parameters
`kafka-s3-connector` is highly configurable. Almost everything can be tweaked and changed by setting the right environment variable before starting the service up. See the full  parameters list and their description under the `conf/application.info` file.

### Writing Custom Mapper Classes
By default, `kafka-s3-connector` is configured to copy the data from Kafka to S3 "as is". However - in most cases you would prefer to transform your Kafka messages before copying them to S3. This can be easily done by creating a new `Mapper` class, like in the following example.

Let's say we want to create a new Mapper class that converts String messages into upper case strings. We can create a new class inside the `com.zooz.kafkas3connector.mapper` package with the following content:

```scala
// com.zooz.kafkas3connector.mapper.UpperCaseMapper

package com.zooz.kafkas3connector.mapper

import java.nio.ByteBuffer
import com.zooz.kafkas3connector.kafka.bytebufferconverters.StringConverters._

/** A basic message mapper that doesn't perform any transformation on its input */
class UpperCaseMapper extends KafkaMessageMapper{
  override def transformMessage(byteBuffer: ByteBuffer): Seq[ByteBuffer] = {
    val messageAsString: String = byteBuffer.asString
    val messageAsUpperCase: String = messageAsString.toUpperCase()
    val outputMessage: ByteBuffer = messageAsUpperCase.asByteBuffer
    Seq(outputMessage)
  }
}
```
Let's briefly go over the example:
1. We created a new scala class, called `UpperCaseMapper` which extends the abstract `KafkaMessageMapper` class and override its `transformMessage(byteBuffer: ByteBuffer)` method.
2. The `transformMessage` method is called for each record read from Kafka. It gets the record's value as a ByteBuffer and is expected to produce a sequence (meaning - zero or more) ByteBuffer records as output.
3. First - we needed to first transform the input form ByteBuffer into a string. To do that, we imported the `StringConverters` object from the `bytebufferconverters` package. This object provides us with `byteBuffer.asString` and `string.asByteBuffer` methods to transform ByteBuffer instances to and from strings. There is a similar `JsObjectToByteBuffer` object with similar support for Play's JsObject (Json) data-types. If your Kafka messages are neither strings nor JSONs - than you need to write your own conversions.
4. Once we had our input as a String - we turned it into upper-case, converted it back into a ByteBuffer and returned a seqence with our output as the only member.

This is just a basic example, but you can implement any complex mapping logic in the same manner. You should note that:
1. A single input can be converted into multiple output records. This is the reason `transformMessage` returns a sequence of byte-buffer records.
2. Similarly - you can use your mappers to filter data out. Just return an empty `Seq[ButeBuffer]()` value for the messages you want to ommit.

### Writing Service Loaders
Now that you have your custom mapper - you want `kafka-s3-connector` to use it instead of the default mapper. The way to do it is to create a custom service loader. 

Create a new class inside the `com.zooz.kafkas3connector.modules` package with the following content:

```scala
// com.zooz.kafkas3connector.modules.UpperCaseLoader

package com.zooz.kafkas3connector.modules

import play.api.{Configuration, Environment}
import com.zooz.kafkas3connector.mapper.KafkaMessageMapper
import com.zooz.kafkas3connector.mapper.UpperCaseMapper

class UpperCaseLoader(environment: Environment, configuration: Configuration)
    extends DefaultLoader(environment, configuration) {

  override def bindMessageMapper: Unit = {
    bind(classOf[KafkaMessageMapper]).to(classOf[UpperCaseMapper])
  }
}
```

Let's review:
1. We created a new class called `UpperCaseLoader` which extends the `DefaultLoader` class.
2. `DefaultLoader` is a Play framework module. Play expects all its modules to accept `Environment` and `Configuration` parameters to for their constructor. Just make sure to accept those parameters and pass them on to the parent `DefaultLoader` class.
3. We've overridden the `bindMessageMapper` class and told it to bind our own `UpperCaseMapper` to any instance of `KafkaMessageMapper` that appear in the code.

Now that we have our own service loader - we need to tell `kafka-s3-connector` to use it instead of the default loader. We do this by setting the `LOADING_MODULE`environment variable:

```sh
export LOADING_MODULE=com.zooz.kafkas3connector.modules.UpperCaseLoader
```

## Advanced customization
In most cases - writing your own mappers would be sufficient. However - `kafka-s3-connector` allows you to customize two more components:
* __BuffersManager__ - responsible for directing each message into the path S3.
* __HealthChecker__ - Affecting the result returned by the `/health` endpoint, and hence for the monitoring of the service's health.

### Writing Custom BuffersManager
First - let's understand the role of the `BuffersManager` classes.

`kafka-s3-connector` guarantees an __exactly-once__ storage of messages to S3. It does so by:
1. Not committing to Kafka before messages are safely stored in S3.
2. Message idempotency - each message is guaranteed to go to the same output file even if it was read more than once. Idempotency allows us to avoid duplicates in S3 if the service failed before committing to Kafka.

The `BuffersManager` class is responsible to guarantee #2. 

Messages will always be stored in S3 according to the following path:

```
s3://[bucket]/[root path]/date=[YYYY-mm-dd]/hour=[HH]/[partition]_[offset of first message].[extension]
```

Some parts of the output path (like _bucket_, _root path_ and the _date_ and _hour_ strings) are configurable. However - The date, the hour and the offset are part of the idempotency mechanism and cannot be changed.

By default - the date and time used for the output are based on the creation date of the Kafka record. If you want to extract the date and time from the record's value instead - you can provide your own BuffersManager class. 

As an example - let's consider a case where our input is constructed of logging messages, similar to the following:
```
2019-03-12 12:00:01.003 - INFO - service started
2019-03-12 12:00:01.006 - WARNING - Not all environment variables were set - assuming defaults
2019-03-12 12:00:01.017 - INFO - Proceeding with startup
```
We would like our messages to be stored in S3 according to their actual date and time, and not based on the time they were placed into Kafka. We can do this by creating our `BuffersManager` class:

```scala
// package com.zooz.kafkas3connector.buffermanagers.s3buffermanagers.LoggingBuffersManager

package com.zooz.kafkas3connector.buffermanagers.s3buffermanagers

import javax.inject.Inject
import play.api.Configuration
import javax.inject.Singleton
import com.zooz.kafkas3connector.buffermanagers.BufferManagerMetrics
import com.zooz.kafkas3connector.kafka.KafkaMessage
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormatter
import org.joda.time.format.DateTimeFormat
import com.zooz.kafkas3connector.kafka.bytebufferconverters.StringConverters._

@Singleton
class LoggingBuffersManager @Inject()(
    config: Configuration,
    metrics: BufferManagerMetrics
) extends S3BufferManager(config, metrics) {
  
  val dateFormatter: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS")

  override def getDateTimeFromMessage(kafkaMessage: KafkaMessage): Option[DateTime] = {
    val messageAsString: String = kafkaMessage.value.asString

    // Cutting the first 23 characters from the message, containing the timestamp
    val tsString: String = messageAsString.substring(0,23)
    try {
      Some(dateFormatter.parseDateTime(tsString))
    } catch {
      case parseError: java.lang.IllegalArgumentException =>
        // Failed to parse the date - returning None
        None
    }
  }
}
```

Let's go over the example:
1. We created a new class called `LoggingBuffersManager` which extends `S3BufferManager` (itself  an instance of `BuffersManager`). 
2. There should be only a single instance of `LoggingBuffersManager` and hence we need to use the `@Singleton` annotation.
2. `S3BufferManager` needs access Play's configuration and prometheus metrics, so we've asked Play to `@Inject` those to our class and passed them on.
3. We've overriden the `getDateTimeFromMessage` method. This method will be called for each `KafaMessage` processed and is expected to produce an `Option[org.joda.time.DateTime]` object as an output.
4. Inside the method - we've converted the message's value into a string (using the `StringConverters` implicits), extracted the date and time substring and parsed it into a `DateTime` object. In case of an error - we're expected to return `None`.

We now need to create a new Service loader to load our custom BuffersManager class:
```scala
// com.zooz.kafkas3connector.modules.LoggingInputLoader

package com.zooz.kafkas3connector.modules

import play.api.{Configuration, Environment}
import com.zooz.kafkas3connector.buffermanagers.s3buffermanagers.LoggingBuffersManager

class LoggingInputLoader(environment: Environment, configuration: Configuration)
    extends DefaultLoader(environment, configuration) {

  override def bindBufferManager: Unit = {
    bindActor[LoggingBuffersManager]("buffers-manager")
  }
}
```
In this case - we've overriden the `bindBufferManager` to bind our own `LoggingBuffersManager` class to any reference to the `buffers-manager` Akka actor.

To use our service loader on startup - we need to set the `LOADING_MODULE`environment variable:

```sh
export LOADING_MODULE=com.zooz.kafkas3connector.modules.LoggingInputLoader’
```

### Writing Custom HealthChecker
By default - `kafka-s3-connector` health is determined based on the lag between the last message it read from Kafka and the latest offset of this topic. It is possible to add additional custom checks by creating a custom `HealthChecker` class and override the `bindHealthChecker` method of the `DefaultLoader` class. 

Check the implementation of the `com.zooz.kafkas3connector.health.KafkaHealthChecker` class to learn more.

## Maintainers
The current maintainers (people who can merge pull requests) are:
* [NoamGutHub](https://github.com/NoamGitHub)
* [guybiecher](https://github.com/guybiecher)
