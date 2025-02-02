package pl.touk.nussknacker.engine.lite.kafka

import cats.implicits.toTraverseOps
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.{AuthorizationException, InterruptException, OutOfOrderSequenceException, ProducerFencedException}
import pl.touk.nussknacker.engine.api.runtimecontext.EngineRuntimeContext
import pl.touk.nussknacker.engine.api.{Context, MetaData}
import pl.touk.nussknacker.engine.kafka.KafkaUtils
import pl.touk.nussknacker.engine.kafka.exception.KafkaJsonExceptionSerializationSchema
import pl.touk.nussknacker.engine.lite.ScenarioInterpreterFactory.ScenarioInterpreterWithLifecycle
import pl.touk.nussknacker.engine.lite.api.commonTypes.{ErrorType, ResultType}
import pl.touk.nussknacker.engine.lite.api.interpreterTypes
import pl.touk.nussknacker.engine.lite.api.interpreterTypes.{ScenarioInputBatch, SourceId}
import pl.touk.nussknacker.engine.lite.kafka.KafkaTransactionalScenarioInterpreter.{EngineConfig, Output}
import pl.touk.nussknacker.engine.lite.kafka.api.LiteKafkaSource
import pl.touk.nussknacker.engine.lite.metrics.SourceMetrics
import pl.touk.nussknacker.engine.util.exception.WithExceptionExtractor

import java.util.UUID
import scala.compat.java8.DurationConverters.FiniteDurationops
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.jdk.CollectionConverters.{asJavaCollectionConverter, asScalaIteratorConverter, iterableAsScalaIterableConverter, mapAsJavaMapConverter}
import scala.util.control.NonFatal

class KafkaSingleScenarioTaskRun(taskId: String,
                                 metaData: MetaData,
                                 runtimeContext: EngineRuntimeContext,
                                 engineConfig: EngineConfig,
                                 interpreter: ScenarioInterpreterWithLifecycle[Future, Output],
                                 sourceMetrics: SourceMetrics)
                                (implicit ec: ExecutionContext) extends Task with LazyLogging {

  private val groupId = metaData.id

  private var consumer: KafkaConsumer[Array[Byte], Array[Byte]] = _
  private var producer: KafkaProducer[Array[Byte], Array[Byte]] = _

  private var consumerMetricsRegistrar: KafkaMetricsRegistrar = _
  private var producerMetricsRegistrar: KafkaMetricsRegistrar = _

  private val sourceToTopic: Map[String, Map[SourceId, LiteKafkaSource]] = interpreter.sources.flatMap {
    case (sourceId, kafkaSource: LiteKafkaSource) =>
      kafkaSource.topics.map(topic => topic -> (sourceId, kafkaSource))
    case (sourceId, other) => throw new IllegalArgumentException(s"Unexpected source: $other for ${sourceId.value}")
  }.groupBy(_._1).mapValues(_.values.toMap)

  def init(): Unit = {
    configSanityCheck()

    consumer = prepareConsumer
    producer = prepareProducer
    producer.initTransactions()
    consumer.subscribe(sourceToTopic.keys.toSet.asJavaCollection)

    registerMetrics()
  }

  private def prepareConsumer: KafkaConsumer[Array[Byte], Array[Byte]] = {
    val properties = KafkaUtils.toTransactionalAwareConsumerProperties(engineConfig.kafka, Some(groupId))
    // offset commit is done manually via sendOffsetsToTransaction
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
    new KafkaConsumer[Array[Byte], Array[Byte]](properties)
  }

  private def prepareProducer: KafkaProducer[Array[Byte], Array[Byte]] = {
    val producerProps = KafkaUtils.toProducerProperties(engineConfig.kafka, groupId)
    //FIXME generate correct id - how to connect to topic/partition??
    producerProps.put("transactional.id", groupId + UUID.randomUUID().toString)
    new KafkaProducer[Array[Byte], Array[Byte]](producerProps)
  }

  private def registerMetrics(): Unit = {
    consumerMetricsRegistrar = new KafkaMetricsRegistrar(taskId, consumer.metrics(), runtimeContext.metricsProvider)
    consumerMetricsRegistrar.registerMetrics()
    producerMetricsRegistrar = new KafkaMetricsRegistrar(taskId, producer.metrics(), runtimeContext.metricsProvider)
    producerMetricsRegistrar.registerMetrics()
  }

  // We have both "mostly" side-effect-less interpreter.invoke and sendOutputToKafka in a body of transaction to avoid situation
  // when beginTransaction fails and we keep restarting interpreter.invoke which can cause e.g. sending many unnecessary requests
  // to rest services. beginTransaction is costless (doesn't communicate with transaction coordinator)
  def run(): Unit = {
    val records = consumer.poll(engineConfig.pollDuration.toJava)
    if (records.isEmpty) {
      logger.trace("No records, skipping")
      return
    }
    producer.beginTransaction()
    try {
      processRecords(records)
      val offsetsMap: Map[TopicPartition, OffsetAndMetadata] = retrieveMaxOffsetsOffsets(records)
      producer.sendOffsetsToTransaction(offsetsMap.asJava, groupId)
      producer.commitTransaction()
    } catch {
      // Those are rather not our cases but their shouldn't cause transaction abortion:
      // https://stackoverflow.com/a/63837803
      case e @ (_: ProducerFencedException | _: OutOfOrderSequenceException | _: AuthorizationException) =>
        logger.warn(s"Fatal producer error: ${e.getMessage}. Closing producer without abort transaction")
        throw e
      case NonFatal(e) =>
        logger.warn(s"Unhandled error: ${e.getMessage}. Aborting kafka transaction")
        producer.abortTransaction()
        throw e
    }
  }

  private def processRecords(records: ConsumerRecords[Array[Byte], Array[Byte]]) = {
    val valuesToRun = prepareRecords(records)
    val output = Await.result(interpreter.invoke(ScenarioInputBatch(valuesToRun)), engineConfig.interpreterTimeout)
    Await.result(sendOutputToKafka(output), engineConfig.publishTimeout)
  }

  private def prepareRecords(records: ConsumerRecords[Array[Byte], Array[Byte]]): List[(SourceId, Context)] = {
    sourceToTopic.toList.flatMap {
      case (topic, sourcesSubscribedOnTopic) =>
        val forTopic = records.records(topic).asScala.toList
        //TODO: try to handle source metrics in more generic way?
        sourcesSubscribedOnTopic.keys.foreach(sourceId => forTopic.foreach(record => sourceMetrics.markElement(sourceId, record.timestamp())))
        sourcesSubscribedOnTopic.mapValues(source => forTopic.map(source.deserialize(runtimeContext, _))).toList.flatMap {
          case (sourceId, contexts) => contexts.map((sourceId, _))
        }
    }
  }

  private def sendOutputToKafka(output: ResultType[interpreterTypes.EndResult[ProducerRecord[Array[Byte], Array[Byte]]]]): Future[_] = {
    val results = output.value.map(_.result)
    val errors = output.written.map(serializeError)
    (results ++ errors).map(KafkaUtils.sendToKafka(_)(producer)).sequence
  }

  //TODO: test behaviour on transient exceptions
  private def serializeError(error: ErrorType): ProducerRecord[Array[Byte], Array[Byte]] = {
    val nonTransient = WithExceptionExtractor.extractOrThrow(error)
    KafkaJsonExceptionSerializationSchema(metaData, engineConfig.exceptionHandlingConfig).serialize(nonTransient)
  }

  // See https://www.baeldung.com/kafka-exactly-once for details
  private def retrieveMaxOffsetsOffsets(records: ConsumerRecords[Array[Byte], Array[Byte]]): Map[TopicPartition, OffsetAndMetadata] = {
    records.iterator().asScala.map { rec =>
      val upcomingOffset = rec.offset() + 1
      (new TopicPartition(rec.topic(), rec.partition()), upcomingOffset)
    }.toList.groupBy(_._1).mapValues(_.map(_._2).max).mapValues(new OffsetAndMetadata(_))
  }

  //Errors from this method will be considered as fatal, handled by uncaughtExceptionHandler and probably causing System.exit
  def close(): Unit = {
    List(producer, consumer, producerMetricsRegistrar, consumerMetricsRegistrar)
      .filter(_ != null)
      .foreach(closeable => retryCloseOnInterrupt(closeable.close))
    logger.info(s"Closed runner for ${metaData.id}")
  }

  private def configSanityCheck(): Unit = {
    val properties = KafkaUtils.toTransactionalAwareConsumerProperties(engineConfig.kafka, None)
    val maxPollInterval = new ConsumerConfig(properties).getInt(CommonClientConfigs.MAX_POLL_INTERVAL_MS_CONFIG)
    if (maxPollInterval <= (engineConfig.interpreterTimeout + engineConfig.publishTimeout).toMillis) {
      throw new IllegalArgumentException(s"publishTimeout + interpreterTimeout cannot exceed " +
        s"${CommonClientConfigs.MAX_POLL_INTERVAL_MS_CONFIG}")
    }
  }

  //it may happen that interrupt signal will be mixed in closing process. We want to close
  //normally, so we retry close action - but only once, as we expect only one interrupt call
  private def retryCloseOnInterrupt(action: () => Unit): Unit = {
    try {
      action()
    } catch {
      case _: InterruptedException | _: InterruptException  =>
        //This is important - as it's the only way to clear interrupted flag...
        val wasInterrupted = Thread.interrupted()
        logger.debug(s"Interrupted during close: $wasInterrupted, trying once more")
        action()
    }
  }

}
