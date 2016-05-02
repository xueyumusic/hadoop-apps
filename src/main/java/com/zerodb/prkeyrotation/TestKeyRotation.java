package com.zerodb.prkeyrotation;

import com.zerodb.crypto.BBS98Engine;

public class TestKeyRotation {
	public static void main(String[] args) {
		System.out.println("##log from testkey rotation container");
		System.out.println("##modification one");
		System.out.println("##begin jep");
		BBS98Engine engine = new BBS98Engine();
		String g = engine.generateParam();
		System.out.println("##param g:"+g);
	}
}
