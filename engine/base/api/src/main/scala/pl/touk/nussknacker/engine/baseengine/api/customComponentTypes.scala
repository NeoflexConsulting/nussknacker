package pl.touk.nussknacker.engine.baseengine.api

import cats.data.ValidatedNel
import cats.{Monad, ~>}
import pl.touk.nussknacker.engine.api.context.ProcessCompilationError
import pl.touk.nussknacker.engine.api.process.Sink
import pl.touk.nussknacker.engine.api.runtimecontext.ContextIdGenerator
import pl.touk.nussknacker.engine.api.typed.typing.TypingResult
import pl.touk.nussknacker.engine.api.{Context, LazyParameterInterpreter}
import pl.touk.nussknacker.engine.baseengine.api.commonTypes.{DataBatch, ResultType}

import scala.language.higherKinds
import scala.reflect.runtime.universe._

object customComponentTypes {

  //Some components work with any monad (e.g. Union, Split etc.) Some require specific monad (e.g. State, probably transactional Kafka)
  //This trait allows to convert effects if it's possible. See sample.SumTransformer for usage
  //More complex implementations would allow e.g. to transform State[StateType, _] => Future[State[StateType, _]] and so on
  trait CapabilityTransformer[Target[_]] {
    def transform[From[_]](implicit tag: TypeTag[From[Any]]): ValidatedNel[ProcessCompilationError, From ~> Target]
  }

  // TODO: we should open components other then services (sources, custom nodes, sinks) instead of passing ContextIdGenerator here
  case class CustomComponentContext[F[_]](nodeId: String, interpreter: LazyParameterInterpreter, capabilityTransformer: CapabilityTransformer[F], contextIdGenerator: ContextIdGenerator)

  trait CustomBaseEngineComponent {

    //Result is generic parameter, as Component should not change it/interfer with it
    def createTransformation[F[_] : Monad, Result](continuation: DataBatch => F[ResultType[Result]],
                                                   context: CustomComponentContext[F]): DataBatch => F[ResultType[Result]]

  }

  case class BranchId(value: String)

  case class JoinDataBatch(value: List[(BranchId, Context)])


  trait JoinCustomBaseEngineComponent {

    def createTransformation[F[_] : Monad, Result](continuation: DataBatch => F[ResultType[Result]],
                                                   context: CustomComponentContext[F]): JoinDataBatch => F[ResultType[Result]]

  }

  trait BaseEngineSink[Res] extends Sink {
    def createTransformation[F[_] : Monad](evaluateLazyParameter: CustomComponentContext[F]):
      (TypingResult, DataBatch => F[ResultType[(Context, Res)]])
  }



}
