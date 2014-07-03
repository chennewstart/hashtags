package src.test.java.hashtags;

import java.util.ArrayList;
import java.util.List;

public class Test {

	public static void main(String[] args) {
		ArrayList<Integer> a = new ArrayList<Integer>();
		a.add(0);
		a.add(2);
		a.add(4);
		List<Integer> b = a;
		a.clear();
		for (Integer i : a) {
			System.out.println(i);
		}
		System.out.println("wulala");
		for (Integer i : b) {
			System.out.println(i);
		}
	}

}
