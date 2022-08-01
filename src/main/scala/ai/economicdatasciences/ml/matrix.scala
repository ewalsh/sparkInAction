package ai.economicdatasciences.sia.ml

import org.apache.spark.mllib.linalg.{Vectors, Vector, DenseVector, SparseVector, DenseMatrix, SparseMatrix, Matrix, Matrices}
import org.apache.spark.mllib.linalg.distributed.{RowMatrix, CoordinateMatrix, BlockMatrix, DistributedMatrix, MatrixEntry}
import breeze.linalg.{DenseVector => BDV, SparseVector => BSV, Vector => BV, DenseMatrix => BDM, CSCMatrix => BSM, Matrix => BM}

object MLMatrix {
  val dv1 = Vectors.dense(5.0, 6.0, 7.0, 8.0)
  val dv2 = Vectors.dense(Array(5.0, 6.0, 7.0, 8.0))
  val sv = Vectors.sparse(4, Array(0,1,2,3), Array(5.0, 6.0, 7.0, 8.0))
  val dm = Matrices.dense(2,3,Array(5.0,0.0,0.0,3.0,1.0,4.0))
  val sm = Matrices.sparse(2,3, Array(0,1,2,4), Array(0,1,0,1), Array(5,3,1,4))

  def toBreezeV(v: Vector): BV[Double] = v match {
    case dv: DenseVector => new BDV(dv.values)
    case sv: SparseVector => new BSV(sv.indices, sv.values, sv.size)
  }

  def toBreezeM(m:Matrix): BM[Double] = m match {
    case dm: DenseMatrix => new BDM(dm.numRows, dm.numCols, dm.values)
    case sm: SparseMatrix => new BSM(sm.values, sm.numCols, sm.numRows, sm.colPtrs, sm.rowIndices)
  }

  def toBreezeD(dm: DistributedMatrix): BM[Double] = dm match {
    case rm: RowMatrix => {
      val m = rm.numRows().toInt
       val n = rm.numCols().toInt
       val mat = BDM.zeros[Double](m, n)
       var i = 0
       rm.rows.collect().foreach { vector =>
         for(j <- 0 to vector.size-1)
         {
           mat(i, j) = vector(j)
         }
         i += 1
       }
       mat
     }
    case cm: CoordinateMatrix => {
       val m = cm.numRows().toInt
       val n = cm.numCols().toInt
       val mat = BDM.zeros[Double](m, n)
       cm.entries.collect().foreach { case MatrixEntry(i, j, value) =>
         mat(i.toInt, j.toInt) = value
       }
       mat
    }
    case bm: BlockMatrix => {
       val localMat = bm.toLocalMatrix()
       new BDM[Double](localMat.numRows, localMat.numCols, localMat.toArray)
    }
  }

  //UTILITY METHOD FOR PRETTY-PRINTING MATRICES
  def printMat(mat: BM[Double]) = {
     print("            ")
     for(j <- 0 to mat.cols-1) print("%-10d".format(j));
     println
     for(i <- 0 to mat.rows-1) { print("%-6d".format(i)); for(j <- 0 to mat.cols-1) print(" %+9.3f".format(mat(i, j))); println }
  }
}
