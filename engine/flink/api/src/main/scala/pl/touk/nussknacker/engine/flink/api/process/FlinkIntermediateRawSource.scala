package pl.touk.nussknacker.engine.flink.api.process

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import pl.touk.nussknacker.engine.api.Context
import pl.touk.nussknacker.engine.api.process.{BasicContextInitializer, ContextInitializer, Source}
import pl.touk.nussknacker.engine.api.typed.typing.Unknown
import pl.touk.nussknacker.engine.flink.api.compat.ExplicitUidInOperatorsSupport
import pl.touk.nussknacker.engine.flink.api.timestampwatermark.TimestampWatermarkHandler

/**
  * Source with typical source stream trasformations:
  *  1. adds source using provided SourceFunction (here raw source produces raw data)
  *  2. sets UID
  *  3. assigns timestamp and watermarks
  *  4. initializes Context that is streamed within output DataStream (here raw data are mapped to Context)
  * It separates raw event data produced by SourceFunction and data released to the stream as Context variables.
  * By default it uses basic "single input value" implementation of initializer, see [[BasicContextInitializer]].
  *
  * @tparam Raw - type of raw event that is generated by flink source function.
  */
trait FlinkIntermediateRawSource[Raw] extends ExplicitUidInOperatorsSupport { self: Source =>

  // We abstracting to stream so theoretically it shouldn't be defined on this level but:
  // * for test mechanism purpose we need to know what type will be generated.
  // * for production sources (eg BaseFlinkSource, KafkaSource) it is used to determine type information for flinkSourceFunction
  def typeInformation: TypeInformation[Raw]

  def timestampAssigner : Option[TimestampWatermarkHandler[Raw]]

  val contextInitializer: ContextInitializer[Raw] = new BasicContextInitializer[Raw](Unknown)

  def prepareSourceStream(env: StreamExecutionEnvironment, flinkNodeContext: FlinkCustomNodeContext, sourceFunction: SourceFunction[Raw]): DataStream[Context] = {

    //1. add source and 2. set UID
    val rawSourceWithUid = setUidToNodeIdIfNeed(flinkNodeContext, env
      .addSource[Raw](sourceFunction)(typeInformation)
      .name(flinkNodeContext.nodeId))

    //3. assign timestamp and watermark policy
    val rawSourceWithUidAndTimestamp = timestampAssigner
      .map(_.assignTimestampAndWatermarks(rawSourceWithUid))
      .getOrElse(rawSourceWithUid)

    //4. initialize Context and spool Context to the stream
    val typeInformationFromNodeContext = flinkNodeContext.typeInformationDetection.forContext(flinkNodeContext.validationContext.left.get)
    rawSourceWithUidAndTimestamp
      .map(new RichLifecycleMapFunction[Raw, Context](
        contextInitializer.initContext(flinkNodeContext.nodeId),
        flinkNodeContext.convertToEngineRuntimeContext))(typeInformationFromNodeContext)
  }
}