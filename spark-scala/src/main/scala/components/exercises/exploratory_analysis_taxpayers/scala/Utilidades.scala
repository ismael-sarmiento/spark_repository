package components.exercises.exploratory_analysis_taxpayers.scala

import components.exercises.exploratory_analysis_taxpayers.Contribuyente

import scala.io.Source.fromFile
import scala.util.{Failure, Success, Try}

object Utilidades {

  val filePath = "src/main/scala/components/exercises/exploratory_analysis_taxpayers/resources/adult.data.clean.csv"

  def loadDataset(file: String = filePath): Seq[Contribuyente] = {

    def getDefaultInt(v: String) = Try(v.toInt) match {
      case Success(value) => value
      case Failure(_) => 0
    }

    val allRows = fromFile(file).getLines().filterNot(_ == "").toSeq
    val rowsWithoutHead = allRows.slice(1, allRows.length)

    val rows = rowsWithoutHead.map(
      line => {
        val Array(age, workclass, education, educationNum, maritalStatus, occupation, relationship, race, sex, capitalGain, capitalLoss, hoursPerWeek, nativeCountry, income) = line.split(",")
        Contribuyente(
          age = getDefaultInt(age),
          workclass = workclass.replace("\"", ""),
          education = education.replace("\"", ""),
          educationNum = getDefaultInt(educationNum),
          maritalStatus = maritalStatus.replace("\"", ""),
          occupation = occupation.replace("\"", ""),
          relationship = relationship.replace("\"", ""),
          race = race.replace("\"", ""),
          sex = sex.replace("\"", ""),
          capitalGain = getDefaultInt(capitalGain),
          capitalLoss = getDefaultInt(capitalLoss),
          hoursPerWeek = getDefaultInt(hoursPerWeek),
          nativeCountry = nativeCountry.replace("\"", ""),
          income = income.replace("\"", "")
        )
      }
    )
    rows
  }
}
