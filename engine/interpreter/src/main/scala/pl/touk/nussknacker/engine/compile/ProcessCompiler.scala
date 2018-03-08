package pl.touk.nussknacker.engine.compile

import cats.data.Validated._
import cats.data.{NonEmptyList, Validated, ValidatedNel}
import cats.instances.list._
import org.apache.commons.lang3.ClassUtils
import pl.touk.nussknacker.engine._
import pl.touk.nussknacker.engine.api.MetaData
import pl.touk.nussknacker.engine.api.exception.{EspExceptionHandler, EspExceptionInfo}
import pl.touk.nussknacker.engine.api.process._
import pl.touk.nussknacker.engine.api.typed.ClazzRef
import pl.touk.nussknacker.engine.api.typed.typing.{Typed, TypingResult}
import pl.touk.nussknacker.engine.canonicalgraph.CanonicalProcess
import pl.touk.nussknacker.engine.canonize.ProcessCanonizer
import pl.touk.nussknacker.engine.compile.ProcessCompilationError._
import pl.touk.nussknacker.engine.compile.dumb._
import pl.touk.nussknacker.engine.compiledgraph.CompiledProcessParts
import pl.touk.nussknacker.engine.compiledgraph.part.NextWithParts
import pl.touk.nussknacker.engine.definition.DefinitionExtractor._
import pl.touk.nussknacker.engine.definition.ProcessDefinitionExtractor.{CustomTransformerAdditionalData, ProcessDefinition}
import pl.touk.nussknacker.engine.definition._
import pl.touk.nussknacker.engine.expression.ExpressionEvaluator
import pl.touk.nussknacker.engine.graph.exceptionhandler.ExceptionHandlerRef
import pl.touk.nussknacker.engine.graph.expression.Expression
import pl.touk.nussknacker.engine.graph.node.SubprocessInputDefinition.SubprocessParameter
import pl.touk.nussknacker.engine.graph.node.{CustomNode, StartingNodeData, SubprocessInputDefinition}
import pl.touk.nussknacker.engine.graph.sink.SinkRef
import pl.touk.nussknacker.engine.graph.{EspProcess, evaluatedparam}
import pl.touk.nussknacker.engine.split._
import pl.touk.nussknacker.engine.splittedgraph._
import pl.touk.nussknacker.engine.splittedgraph.part._
import pl.touk.nussknacker.engine.splittedgraph.splittednode.{Next, NextNode, PartRef, SplittedNode}
import pl.touk.nussknacker.engine.util.Implicits._

import scala.util.Try
import scala.util.control.NonFatal

class ProcessCompiler( protected val classLoader: ClassLoader,
                       protected val sub: PartSubGraphCompilerBase,
                      protected val definitions: ProcessDefinition[ObjectWithMethodDef]) extends ProcessCompilerBase {

  //FIXME: should it be here?
  private val expressionEvaluator = {
    val globalVars = definitions.expressionConfig.globalVariables.mapValuesNow(_.obj)
    ExpressionEvaluator.withoutLazyVals(globalVars, List())
  }

  override type ParameterProviderT = ObjectWithMethodDef

  override def compile(process: EspProcess): CompilationResult[CompiledProcessParts] = {
    super.compile(process)
  }

  override protected def createCustomNodeInvoker(obj: ObjectWithMethodDef, metaData: MetaData, node: SplittedNode[graph.node.CustomNode]) =
    CustomNodeInvoker[Any](obj, metaData, node)

  override protected def createFactory[T](obj: ObjectWithMethodDef) =
    ProcessObjectFactory[T](obj, expressionEvaluator)

}

class ProcessValidator(protected val classLoader: ClassLoader,
                       protected val sub: PartSubGraphCompilerBase,
                       protected val definitions: ProcessDefinition[ObjectDefinition]) extends ProcessCompilerBase {

  override type ParameterProviderT = ObjectDefinition

  override protected def createFactory[T](obj: ObjectDefinition) =
    new DumbProcessObjectFactory[T]

  override protected def createCustomNodeInvoker(obj: ObjectDefinition, metaData: MetaData, node: SplittedNode[graph.node.CustomNode]) =
    new DumbCustomNodeInvoker[Any]
}

protected trait ProcessCompilerBase {

  type ParameterProviderT <: ObjectMetadata

  protected def definitions: ProcessDefinition[ParameterProviderT]
  protected def sourceFactories = definitions.sourceFactories
  protected def sinkFactories = definitions.sinkFactories
  protected def exceptionHandlerFactory = definitions.exceptionHandlerFactory
  protected val customStreamTransformers = definitions.customStreamTransformers

  protected def sub: PartSubGraphCompilerBase

  private val syntax = ValidatedSyntax[ProcessCompilationError]
  import syntax._

  protected def classLoader: ClassLoader

  private val expressionCompiler = ExpressionCompiler.withoutOptimization(classLoader, definitions.expressionConfig)

  def validate(canonical: CanonicalProcess): CompilationResult[Unit] = {
    //TODO: typing not canonical processs... in most cases (wrong tail) it's easy
    ProcessCanonizer.uncanonize(canonical).fold(k => CompilationResult(Invalid(k)), validate)
  }

  def validate(process: EspProcess): CompilationResult[Unit] = {
    try {
      compile(process).map(_ => Unit)
    } catch {
      case NonFatal(e) =>
        CompilationResult(Invalid(NonEmptyList.of(FatalUnknownError(e.getMessage))))
    }
  }

  protected def compile(process: EspProcess): CompilationResult[CompiledProcessParts] = {
    compile(ProcessSplitter.split(process))
  }

  private def compile(splittedProcess: SplittedProcess): CompilationResult[CompiledProcessParts] = {
    implicit val metaData = splittedProcess.metaData
    CompilationResult.map3(
      CompilationResult(findDuplicates(splittedProcess.source).toValidatedNel),
      CompilationResult(compile(splittedProcess.exceptionHandlerRef)),
      compile(splittedProcess.source)
    ) { (_, exceptionHandler, source) =>
      CompiledProcessParts(splittedProcess.metaData, exceptionHandler, source)
    }
  }

  private def findDuplicates(part: SourcePart): Validated[ProcessCompilationError, Unit] = {
    val allNodes = NodesCollector.collectNodesInAllParts(part)
    val duplicatedIds =
      allNodes.map(_.id).groupBy(identity).collect {
        case (id, grouped) if grouped.size > 1 =>
          id
      }
    if (duplicatedIds.isEmpty)
      valid(Unit)
    else
      invalid(DuplicatedNodeIds(duplicatedIds.toSet))
  }

  private def contextAfterCustomNode(node: CustomNode, nodeDefinition: ParameterProviderT, validationContext: ValidationContext, clearsContext: Boolean)
                                    (implicit nodeId: NodeId): ValidatedNel[ProcessCompilationError, ValidationContext] = {
    val maybeClearedContext = if (clearsContext) validationContext.copy(variables = Map()) else validationContext
    (node.outputVar, nodeDefinition.hasNoReturn) match {
      case (Some(varName), false) => maybeClearedContext.withVariable(varName, nodeDefinition.returnType)
        //ble... NonEmptyList is invariant...
        .asInstanceOf[ValidatedNel[ProcessCompilationError,ValidationContext]]
      case (None, true) => Valid(maybeClearedContext)
      case (Some(_), true) => Invalid(NonEmptyList.of(RedundantParameters(Set("OutputVariable"))))
      case (None, false) => Invalid(NonEmptyList.of(MissingParameters(Set("OutputVariable"))))
    }
  }

  private def compile(part: SubsequentPart, ctx: ValidationContext)
                     (implicit metaData: MetaData): CompilationResult[compiledgraph.part.SubsequentPart] = {
    implicit val nodeId = NodeId(part.id)
    part match {
      case SinkPart(node) =>
        CompilationResult.map2(sub.validate(node, ctx), CompilationResult(compile(node.data.ref)))((_, obj) =>
          compiledgraph.part.SinkPart(obj, node, ctx)
        )
      case CustomNodePart(node, nextParts, ends) =>
        val customNodeDefinition = getCustomNodeDefinition(node)
        val nextCtx = customNodeDefinition.andThen { case (nodeDefinition, additionalData) =>
          contextAfterCustomNode(node.data, nodeDefinition, ctx, additionalData.clearsContext)
        }

        val compiledNode = customNodeDefinition.andThen(n => compileCustomNodeInvoker(node, n._1))
        val nextPartsValidation = sub.validate(node, nextCtx.getOrElse(ctx))

        CompilationResult.map4(CompilationResult(compiledNode), nextPartsValidation,
          compile(nextParts, nextPartsValidation.typing), CompilationResult(nextCtx)) { (nodeInvoker, _, nextPartsCompiled, validatedNextCtx) =>
          compiledgraph.part.CustomNodePart(nodeInvoker, node, validatedNextCtx, nextPartsCompiled, ends)
        }.distinctErrors

      case SplitPart(node@splittednode.SplitNode(_, nexts)) =>
        import CompilationResult._

        nexts.map { next =>
          val result = validate(next.next, ctx)
          CompilationResult.map2(result, compile(next.nextParts, result.typing))((_, cp) => NextWithParts(next.next, cp, next.ends))
        }.sequence.map { nextsWithParts =>
          compiledgraph.part.SplitPart(node, ctx, nextsWithParts)
        }

    }
  }

  private def compile(source: SourcePart)
                     (implicit metaData: MetaData): CompilationResult[compiledgraph.part.SourcePart] = {
    implicit val nodeId = NodeId(source.id)
    val variables = computeInitialVariables(source.node.data)
    val initialCtx = ValidationContext(variables)
    val validatedSource = sub.validate(source.node, initialCtx)
    val typesForParts = validatedSource.typing

    CompilationResult.map3(validatedSource, compile(source.nextParts, typesForParts), CompilationResult(compile(source.node.data))) { (_, nextParts, obj) =>
      compiledgraph.part.SourcePart(obj, source.node, initialCtx, nextParts, source.ends)
    }
  }

  private def computeInitialVariables(nodeData: StartingNodeData)(implicit metaData: MetaData, nodeId: NodeId) : Map[String, TypingResult] = nodeData match {
    //TODO: here more elaborate return types (e.g. TypedMap should be handled).
    // Currently it's not easy, as parameters are involved...
    case pl.touk.nussknacker.engine.graph.node.Source(_, ref, _) =>  sourceFactories.get(ref.typ)
          .map(sf => Map(Interpreter.InputParamName -> sf.returnType)).getOrElse(Map.empty)
    case SubprocessInputDefinition(_, params, _) => params.map(p => p.name -> loadFromParameter(p)).toMap
  }

  //TODO: better classloader error handling
  private def loadFromParameter(subprocessParameter: SubprocessParameter)(implicit nodeId: NodeId) =
    Typed(subprocessParameter.typ.toClazzRef(classLoader).getOrElse(throw new IllegalArgumentException(
      s"Failed to load subprocess parameter: ${subprocessParameter.typ.refClazzName} for ${nodeId.id}")))

  private def compile(ref: ExceptionHandlerRef)
                      (implicit metaData: MetaData): ValidatedNel[ProcessCompilationError, EspExceptionHandler] = {
    implicit val nodeId = NodeId(ProcessCompilationError.ProcessNodeId)
    if (metaData.isSubprocess) {
      //FIXME: what should be here?
      Valid(new EspExceptionHandler {
        override def handle(exceptionInfo: EspExceptionInfo[_ <: Throwable]): Unit = {}
      })
    } else {
      compileProcessObject[EspExceptionHandler](exceptionHandlerFactory, ref.parameters)
    }
  }

  private def compile(nodeData: StartingNodeData)
                     (implicit nodeId: NodeId,
                      metaData: MetaData): ValidatedNel[ProcessCompilationError, api.process.Source[Any]] = nodeData match {
    case pl.touk.nussknacker.engine.graph.node.Source(_, ref, _) =>
      val validSourceFactory = sourceFactories.get(ref.typ).map(valid).getOrElse(invalid(MissingSourceFactory(ref.typ))).toValidatedNel
        validSourceFactory.andThen(sourceFactory => compileProcessObject[Source[Any]](sourceFactory, ref.parameters))
    case SubprocessInputDefinition(_, _, _) => Valid(new Source[Any]{}) //FIXME: How should this be handled?
  }

  private def compile(ref: SinkRef)
                     (implicit nodeId: NodeId,
                      metaData: MetaData): ValidatedNel[ProcessCompilationError, api.process.Sink] = {
    val validSinkFactory = sinkFactories.get(ref.typ).map(valid).getOrElse(invalid(MissingSinkFactory(ref.typ))).toValidatedNel
    validSinkFactory.andThen(sinkFactory => compileProcessObject[Sink](sinkFactory, ref.parameters))
  }

  private def compileProcessObject[T](parameterProviderT: ParameterProviderT,
                                      parameters: List[evaluatedparam.Parameter])
                                     (implicit nodeId: NodeId,
                                      metaData: MetaData): ValidatedNel[ProcessCompilationError, T] = {



    expressionCompiler.compileObjectParameters(parameterProviderT.parameters, parameters, Some(ValidationContext.empty)).andThen { compiledParams =>
      validateParameters(parameterProviderT, parameters.map(_.name)).map { _ =>
        val factory = createFactory[T](parameterProviderT)
        factory.create(compiledParams)
      }
    }
  }

  private def getCustomNodeDefinition(node: SplittedNode[graph.node.CustomNode])(implicit nodeId: NodeId, metaData: MetaData) = {
    val ref = node.data.nodeType
    fromOption[ProcessCompilationError, (ParameterProviderT, CustomTransformerAdditionalData)](customStreamTransformers.get(ref), MissingCustomNodeExecutor(ref))
          .toValidatedNel
  }

  private def compileCustomNodeInvoker(node: SplittedNode[CustomNode], nodeDefinition: ParameterProviderT)
                                      (implicit nodeId: NodeId, metaData: MetaData): ValidatedNel[ProcessCompilationError, CustomNodeInvoker[Any]] = {
    validateParameters(nodeDefinition, node.data.parameters.map(_.name))
      .map(createCustomNodeInvoker(_, metaData, node))
  }

  protected def createCustomNodeInvoker(obj: ParameterProviderT, metaData: MetaData, node: SplittedNode[graph.node.CustomNode]) : CustomNodeInvoker[Any]

  protected def createFactory[T](obj: ParameterProviderT): ProcessObjectFactory[T]

  private def compile(parts: List[SubsequentPart], ctx: Map[String, ValidationContext])
                     (implicit metaData: MetaData): CompilationResult[List[compiledgraph.part.SubsequentPart]] = {
    import CompilationResult._
    parts.map(p =>
      ctx.get(p.id).map(compile(p, _)).getOrElse(CompilationResult(Invalid(NonEmptyList.of[ProcessCompilationError](MissingPart(p.id)))))
    ).sequence
  }

  private def validateParameters(parameterProvider: ParameterProviderT, usedParamsNames: List[String])
                                (implicit nodeId: NodeId): ValidatedNel[ProcessCompilationError, ParameterProviderT] = {
    val definedParamNames = parameterProvider.parameters.map(_.name).toSet
    val usedParamNamesSet = usedParamsNames.toSet
    val missingParams = definedParamNames.diff(usedParamNamesSet)
    val redundantParams = usedParamNamesSet.diff(definedParamNames)
    val notMissing = if (missingParams.nonEmpty) invalid(MissingParameters(missingParams)) else valid(Unit)
    val notRedundant = if (redundantParams.nonEmpty) invalid(RedundantParameters(redundantParams)) else valid(Unit)
    A.map2(
      notMissing.toValidatedNel,
      notRedundant.toValidatedNel
    ) { (_, _) => parameterProvider }.leftMap(_.map(identity[ProcessCompilationError]))
  }

  private def validate(n: Next, ctx: ValidationContext): CompilationResult[Unit] = n match {
    case NextNode(node) => sub.validate(node, ctx)
    //TODO: what should be here??
    case PartRef(id) => CompilationResult(Map(id -> ctx), Valid(()))
  }


}

object ProcessValidator {

  def default(definition: ProcessDefinition[ObjectDefinition], loader: ClassLoader = getClass.getClassLoader): ProcessValidator = {
    val expressionCompiler = ExpressionCompiler.withoutOptimization(loader, definition.expressionConfig)
    val sub = new PartSubGraphValidator(loader, expressionCompiler, definition.services)
    new ProcessValidator(loader, sub, definition)
  }

}