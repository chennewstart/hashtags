package hashtags.utils;

import cern.colt.list.DoubleArrayList;
import cern.colt.matrix.impl.SparseDoubleMatrix1D;

/**
 * 
 * @author Michael Vogiatzis (michaelvogiatzis@gmail.com)
 *
 */
public class SparseVector extends SparseDoubleMatrix1D {

	public SparseVector(int size) {
		super(size);
	}

	public SparseVector(double[] values) {
		super(values);
	}

	/**
	 * Returns the Euclid norm which is sum(x[i]^2)
	 * 
	 * @return
	 */
	public double getEuclidNorm() {
		DoubleArrayList dbls = new DoubleArrayList(this.cardinality());
		this.getNonZeros(null, dbls);
		double norm = 0;
		for (int i = 0; i < dbls.size(); i++) {
			norm += Math.pow(dbls.getQuick(i), 2);
		}
		return Math.sqrt(norm);
	}

}
