package components.exercises.exploratory_analysis_taxpayers.spark

case class Contribuyente(
                          age: Int,
                          workClass: String,
                          education: String,
                          educationNum: Int,
                          maritalStatus: String,
                          occupation: String,
                          relationship: String,
                          race: String,
                          sex: String,
                          capitalGain: Int,
                          capitalLoss: Int,
                          hoursPerWeek: Int,
                          nativeCountry: String,
                          income: String
                        )

// Implementa el companion object
// ejercicio-9 (2, 1 cada sub-apartado):
// Dada la clase 'Contribuyente' y es a la que se mapea cada fila del csv, se pide que se cree su companion object y
// definan las funciones:
//  - imprimeDatos que muestre por consola el siguiente formato: "$workclass - $occupation - $nativeCountry - $income"
//  - apply, que no reciba ningún parámetro y que devolverá una instancia de la clase Contribuyente con aquellos campos que sean:
//     del tipo Int inicializados a -1
//     del tipo String inicializado a "desconocido"
object Contribuyente {

  def imprimeDatos(c: Contribuyente): Unit = {
    println(s"${c.workClass} - ${c.occupation} - ${c.nativeCountry} - ${c.income}")
  }

  def apply(): Contribuyente = {
    new Contribuyente(
      age = -1,
      workClass = "desconocido",
      education = "desconocido",
      educationNum = -1,
      maritalStatus = "desconocido",
      occupation = "desconocido",
      relationship = "desconocido",
      race = "desconocido",
      sex = "desconocido",
      capitalGain = -1,
      capitalLoss = -1,
      hoursPerWeek = -1,
      nativeCountry = "desconocido",
      income = "desconocido",
    )
  }
}