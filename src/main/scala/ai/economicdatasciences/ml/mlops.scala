// package ai.economicdatasciences.sia.ml
//
// import org.apache.spark.mllib.linalg.{Vectors, Vector, DenseVector, SparseVector}
// import breeze.linalg.{DenseVector => BDV, SparseVector => BSV, Vector => BV}
//
// trait MLOps {
//   def toBreezeV(v: Vector): BV[Double] = v match {
//     case dv: DenseVector => new BDV(dv.values)
//     case sv: SparseVector => new BSV(sv.indices, sv.values, sv.size)
//   }
// }
