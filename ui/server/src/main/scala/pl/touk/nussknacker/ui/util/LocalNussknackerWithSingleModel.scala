package pl.touk.nussknacker.ui.util

import akka.actor.ActorSystem
import com.typesafe.config.ConfigValueFactory._
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.io.FileUtils
import pl.touk.nussknacker.engine.{DeploymentManagerProvider, ModelData, ProcessingTypeData}
import pl.touk.nussknacker.ui.process.processingtypedata.{BasicProcessingTypeDataReload, MapBasedProcessingTypeDataProvider, ProcessingTypeDataProvider, ProcessingTypeDataReload}
import pl.touk.nussknacker.ui.{NusskanckerDefaultAppRouter, NussknackerAppInitializer}
import sttp.client.{NothingT, SttpBackend}

import java.io.File
import java.nio.file.Files
import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

//This is helper, which allows for starting UI with given model without having to build jar etc.
// See pl.touk.nussknacker.engine.demo.LocalRun in demo/tests for sample usage
object LocalNussknackerWithSingleModel  {

  //default name in config
  val typeName = "streaming"

  def run(modelData: ModelData,
           deploymentManagerProvider: DeploymentManagerProvider,
           managerConfig: Config, categories: Set[String]): Unit = {
    val router = new NusskanckerDefaultAppRouter {
      override protected def prepareProcessingTypeData(config: Config)
                                                      (implicit ec: ExecutionContext, actorSystem: ActorSystem, sttpBackend: SttpBackend[Future, Nothing, NothingT]): (ProcessingTypeDataProvider[ProcessingTypeData], ProcessingTypeDataReload) = {
        //TODO: figure out how to perform e.g. hotswap
        BasicProcessingTypeDataReload.wrapWithReloader(() => {
          val data = ProcessingTypeData.createProcessingTypeData(deploymentManagerProvider, modelData, managerConfig)
          new MapBasedProcessingTypeDataProvider(Map(typeName -> data))
        })
      }
    }
    val file: File = prepareUsersFile()
    val configToUse = ConfigFactory.parseMap(Map[String, Any](
      "authentication.usersFile" -> file.getAbsolutePath,
      "categoriesConfig" -> fromMap(categories.map(cat => cat -> typeName).toMap.asJava)
    ).asJava)
    new NussknackerAppInitializer(configToUse).init(router)
  }

  //TODO: easier way of handling users file
  private def prepareUsersFile(): File = {
    val file = Files.createTempFile("users", "conf").toFile
    FileUtils.write(file,
      """users: [
        |  {
        |    identity: "admin"
        |    password: "admin"
        |    roles: ["Admin"]
        |  }
        |]
        |rules: [
        |  {
        |    role: "Admin"
        |    isAdmin: true
        |  }
        |]
        |""".stripMargin)
    file.deleteOnExit()
    file
  }
}
