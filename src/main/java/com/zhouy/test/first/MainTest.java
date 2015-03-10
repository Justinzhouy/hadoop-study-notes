package com.zhouy.test.first;


public class MainTest {
	 public static void main(String[] args) {
		 String[] tt = "justin 	90".split("\\s+");
		 for (String string : tt) {
			System.out.println(string);
		}
	}
}
