package components.exercises.exploratory_analysis_taxpayers.spark

import components.exercises.exploratory_analysis_taxpayers.spark.Contribuyente.imprimeDatos
import components.exercises.exploratory_analysis_taxpayers.spark.Utilidades.loadDataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import java.lang

object AnalisisExploratorio extends Analizador {

  // ejercicio-1 (0,5):
  // Popula la variable dataset con el resultado de la función loadDataset de Utilidades.
  // Ten en cuenta que se carga el csv completo, incluyendo las cabeceras, asegúrate de omitirlas (la primera fila)
  val dataset: DataFrame = loadDataset()

  dataset.persist()

  // Implementa la función
  // ejercicio-2 (0,5):
  // Número total de registros en el dataset.
  def totalDeRegistros(c: DataFrame): Int = c.count().toInt


  // Implementa la función
  // ejercicio-3 (0,5):
  // Calcular la media de edad de todos los contribuyentes
  def calculaEdadMedia(c: DataFrame): lang.Double = {
    c.select(avg(col("age"))).collectAsList().get(0).get(0).asInstanceOf[java.lang.Double]
  }

  // Implementa la función
  // ejercicio-4 (0,5):
  // Calcular la media de edad de todos los contribuyentes que nunca se han casado.
  // hint: marital-status = Never-Married
  def calculaEdadMediaNeverMarried(c: DataFrame): Double = {
    val maritalStatus: Array[String] = c.select("marital-status").distinct().collect().map(_.get(0).asInstanceOf[String]) // distinct
    val cNeverMarried: Dataset[Row] = c.filter(col("marital-status").equalTo("Never-married"))
    cNeverMarried.select(avg(col("age"))).collectAsList().get(0).get(0).asInstanceOf[java.lang.Double]
  }

  // Implementa la función
  // ejercicio-5 (1):
  // Descubrir de cuántos países distintos provienen los contribuyentes
  def paisesOrigenUnicos(c: DataFrame): Seq[String] = c.select("native-country").distinct().collect().map(_.get(0).asInstanceOf[String]) // distinct

  // Implementa la función
  // ejercicio-6 (1):
  // De todos los contribuyentes, ¿cómo se distribuye por género?. Devuelve el porcentaje de hombres
  // y el de mujeres, en ese orden, (porcentajeDeHombres, porcentajeDeMujeres)
  def distribucionPorGeneros(c: DataFrame): Array[Double] = {
    val groupBySexCount: DataFrame = c.groupBy(col("sex")).agg(count("*").as("countBySex"))
    val groupBySexTotalSex: DataFrame = groupBySexCount.withColumn(
      "totalCount",
      lit(groupBySexCount.agg(sum("countBySex").cast("double")).first.getDouble(0))
    )
    val groupBySexPercentage = groupBySexTotalSex.withColumn(
      "percentage",
      col("countBySex") / col("totalCount") * 100
    )

    val result: Array[Double] = groupBySexPercentage
      .select("percentage")
      .collect()
      .map(r => r.get(0).asInstanceOf[Double])

    result
  }

  // Implementa la función
  // ejercicio-7 (2):
  // Encuentra el tipo de trabajo (workclass) mejor remunerado. El trabajo mejor remunerado es aquel trabajo donde el
  // porcentaje de los contribuyentes que perciben ingresos (income) superiores a ">50K" es mayor que los contribuyentes
  // cuyos ingresos son "<50K".
  def trabajoMejorRemunerado(c: DataFrame): String = {
    val bestWorkClass = c.filter(col("income") === ">50K" && col("workclass")
      .notEqual("")).groupBy(col("workclass"))
      .agg(count("*").as("count"))
      .orderBy(desc("count")).first()
    val bestWorkClassName = bestWorkClass(0).asInstanceOf[String]
    val bestWorkClassValue = bestWorkClass(1).asInstanceOf[Long]
    bestWorkClassName
  }

  // Implementa la función
  // ejercicio-8 (2):
  // Cuál es la media de años de educación (education-num) de aquellos contribuyentes cuyo país de origen no es
  // United-States
  def aniosEstudiosMedio(c: DataFrame): Double = {
    val cWithoutUS: DataFrame = c.filter(col("native-country").notEqual("United-States"))
    cWithoutUS.select(avg(col("education-num"))).collectAsList().get(0).get(0).asInstanceOf[java.lang.Double]
  }

  // ejercicio-11 (1)
  // llama a la función imprimeContribuyentes pasándole los primeros 5 contribuyentes del dataset.
  //    override def imprimeContribuyentes(c: DataFrame): Unit = c.foreach(c => Contribuyente.imprimeDatos(c))

  override def imprimeContribuyentes(c: DataFrame): Unit = {
    //    val personEncoder = Encoders.bean(classOf[Contribuyente])
    //    c.as[Contribuyente](personEncoder)

    c.collect().foreach(r => {
      imprimeDatos(Contribuyente(
        age = r.get(0).asInstanceOf[String].toInt,
        workClass = r.get(1).asInstanceOf[String],
        education = r.get(2).asInstanceOf[String],
        educationNum = r.get(3).asInstanceOf[String].toInt,
        maritalStatus = r.get(4).asInstanceOf[String],
        occupation = r.get(5).asInstanceOf[String],
        relationship = r.get(6).asInstanceOf[String],
        race = r.get(7).asInstanceOf[String],
        sex = r.get(8).asInstanceOf[String],
        capitalGain = r.get(9).asInstanceOf[String].toInt,
        capitalLoss = r.get(10).asInstanceOf[String].toInt,
        hoursPerWeek = r.get(11).asInstanceOf[String].toInt,
        nativeCountry = r.get(12).asInstanceOf[String],
        income = r.get(13).asInstanceOf[String]
      ))
    })

  }


  println(s" -> Dataset tiene un total de registros: ${totalDeRegistros(c = dataset)}")

  println(s" -> En el dataset, los contribuyentes tienen una edad media: ${calculaEdadMedia(c = dataset)}")
  //    println(s" -> En el dataset, los contribuyentes tienen una edad media (sin contar aquellos con age = 0): ${calculaEdadMediaNoZeros(c = dataset)}") // TODO: error en el nombre de la funcion
  println(s" -> En el dataset, los contribuyentes tienen una edad media (contribuyentes sin contar aquellos que nunca se han casado): ${calculaEdadMediaNeverMarried(c = dataset)}")
  println(s" -> Los contribuyentes provien de de distintos países como: ${paisesOrigenUnicos(c = dataset).mkString(",")}")
  println(s" -> Los contribuyentes se distribuyen en (hombres - mujeres): ${distribucionPorGeneros(c = dataset).mkString("Array(", ", ", ")")}")
  println(s" -> El tipo de trabajo mejor remunerado en el dataset es: ${trabajoMejorRemunerado(c = dataset)}")
  println(s" -> La media de años de estudio de los contribuyenes de origen distinto a United States es: ${aniosEstudiosMedio(c = dataset)}")
  // ejercicio-12 (0.5)
  println(s" -> Imprimiendo los 5 primeros contribuyente:")
  imprimeContribuyentes(c = dataset.limit(5))

  dataset.unpersist()
}
