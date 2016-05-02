package com.zerodb.prkeyrotation;

import java.io.PrintWriter;
import java.util.Map;
import java.util.Random;

import com.zerodb.crypto.BBS98Engine;

public class TestMaterialGenerator {
	public static void main(String[] args) throws Exception {
//		Random rand = new Random();
//		byte[] paddingkey = new byte[30];
//		rand.nextBytes(paddingkey);
		
		BBS98Engine engine = new BBS98Engine();
		String g = engine.generateParam();
		
		Map<String, String> keypair = engine.generateKey(g);
		String pk = keypair.get("pk");
		String sk = keypair.get("sk");

		//Random rand = new Random();
		PrintWriter writer = new PrintWriter("testEDEKs.txt", "UTF-8");
		
		for (int i = 0; i < 1000; i++) {
			//System.out.println("##i:"+i);
			
			Random rand = new Random();
			byte[] paddingkey = new byte[30];
			rand.nextBytes(paddingkey);
			if (paddingkey[0] == 0) {
				continue;
			}

			Map<String, String> cipher = engine.encrypt(g, pk, paddingkey);
			writer.write("c1:"+cipher.get("c1"));
			writer.write("\t");
			writer.write("c2:"+cipher.get("c2"));
			writer.write("\n");

		}
		
		writer.close();
		
		Map<String, String> keypairnew = engine.generateKey(g);
		String pknew = keypairnew.get("pk");
		String sknew = keypairnew.get("sk");
		
		String rekey = engine.rekeygen(g, sk, sknew);
		System.out.println("rekey:"+rekey);
		
		
	}

}
