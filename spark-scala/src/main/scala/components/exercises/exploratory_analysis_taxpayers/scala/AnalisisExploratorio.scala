package components.exercises.exploratory_analysis_taxpayers.scala

import components.exercises.exploratory_analysis_taxpayers.Contribuyente
import components.exercises.exploratory_analysis_taxpayers.scala.Utilidades.loadDataset

object AnalisisExploratorio extends Analizador {

  // ejercicio-1 (0,5):
  // Popula la variable dataset con el resultado de la función loadDataset de Utilidades.
  // Ten en cuenta que se carga el csv completo, incluyendo las cabeceras, asegúrate de omitirlas (la primera fila)
  val dataset: Seq[Contribuyente] = loadDataset() // TODO: aqui se puede dejar vacio: explicar

  // Implementa la función
  // ejercicio-2 (0,5):
  // Número total de registros en el dataset.
  def totalDeRegistros(c: Seq[Contribuyente]): Int = c.length


  // Implementa la función
  // ejercicio-3 (0,5):
  // Calcular la media de edad de todos los contribuyentes
  def calculaEdadMedia(c: Seq[Contribuyente]): Double = c.filter(c => !c.age.equals(0)).map(c => c.age).sum / c.length

  // Implementa la función
  // ejercicio-4 (0,5):
  // Calcular la media de edad de todos los contribuyentes que nunca se han casado.
  // hint: marital-status = Never-Married // TODO: "Never-married"
  def calculaEdadMediaNeverMarried(c: Seq[Contribuyente]): Double = {
    val maritalStatus = c.map(c => c.maritalStatus).distinct // TODO: tipos de estado civil
    val cNeverMarried: Seq[Contribuyente] = c.filter(_.maritalStatus == "Never-married")
    cNeverMarried.map(c => c.age).sum / cNeverMarried.length
  }

  // Implementa la función
  // ejercicio-5 (1):
  // Descubrir de cuántos países distintos provienen los contribuyentes
  def paisesOrigenUnicos(c: Seq[Contribuyente]): Seq[String] = c.map(c => c.nativeCountry).distinct

  // Implementa la función
  // ejercicio-6 (1):
  // De todos los contribuyentes, ¿cómo se distribuye por género?. Devuelve el porcentaje de hombres
  // y el de mujeres, en ese orden, (porcentajeDeHombres, porcentajeDeMujeres)
  def distribucionPorGeneros(c: Seq[Contribuyente]): (Double, Double) = {
    val totalSex: Int = c.count(c => c.sex == "Female" || c.sex == "Male")
    val femalePercent: Double = (c.count(c => c.sex == "Female").toDouble / totalSex) * 100
    val malePercent: Double = (c.count(c => c.sex == "Male").toDouble / totalSex) * 100
    (malePercent, femalePercent)
  }

  // Implementa la función
  // ejercicio-7 (2):
  // Encuentra el tipo de trabajo (workclass) mejor remunerado. El trabajo mejor remunerado es aquel trabajo donde el
  // porcentaje de los contribuyentes que perciben ingresos (income) superiores a ">50K" es mayor que los contribuyentes
  // cuyos ingresos son "<50K".
  def trabajoMejorRemunerado(c: Seq[Contribuyente]): String = {
    c.filter(c => c.income == ">50K" && !c.workclass.equals("")).groupBy(_.workclass).map(c => (c._2.length, c._1)).max._2
  }

  // Implementa la función
  // ejercicio-8 (2):
  // Cuál es la media de años de educación (education-num) de aquellos contribuyentes cuyo país de origen no es
  // United-States
  def aniosEstudiosMedio(c: Seq[Contribuyente]): Double = {
    val cWithoutUS: Seq[Contribuyente] = c.filter(c => c.nativeCountry != "United-States")
    cWithoutUS.map(c => c.educationNum).sum / cWithoutUS.length
  }

  // ejercicio-11 (1)
  // llama a la función imprimeContribuyentes pasándole los primeros 5 contribuyentes del dataset.
  override def imprimeContribuyentes(c: Seq[Contribuyente]): Unit = c.foreach(c => Contribuyente.imprimeDatos(c))


  println(s" -> Dataset tiene un total de registros: ${totalDeRegistros(c = dataset)}")
  println(s" -> En el dataset, los contribuyentes tienen una edad media: ${calculaEdadMedia(c = dataset)}")
  //    println(s" -> En el dataset, los contribuyentes tienen una edad media (sin contar aquellos con age = 0): ${calculaEdadMediaNoZeros(c = dataset)}") // TODO: error en el nombre de la funcion
  println(s" -> En el dataset, los contribuyentes tienen una edad media (contribuyentes sin contar aquellos que nunca se han casado): ${calculaEdadMediaNeverMarried(c = dataset)}")
  println(s" -> Los contribuyentes provien de de distintos países como: ${paisesOrigenUnicos(c = dataset).mkString(",")}")
  println(s" -> Los contribuyentes se distribuyen en (hombres - mujeres): ${distribucionPorGeneros(c = dataset)}")
  println(s" -> El tipo de trabajo mejor remunerado en el dataset es: ${trabajoMejorRemunerado(c = dataset)}")
  println(s" -> La media de años de estudio de los contribuyenes de origen distinto a United States es: ${aniosEstudiosMedio(c = dataset)}")
  // ejercicio-12 (0.5)
  println(s" -> Imprimiendo los 5 primeros contribuyente:")
  imprimeContribuyentes(c = dataset.slice(0, 5))
}
