package hashtags.utils;

/**
 * @author Michael Vogiatzis (michaelvogiatzis@gmail.com)
 */
public class DotProduct {

	private double value;
	private int position;

	public DotProduct(double val, int pos) {
		value = val;
		position = pos;
	}

	public double getValue() {
		return value;
	}

	public int getPosition() {
		return position;
	}

}
