/*
 * ShaTest.java
 *
 * Tigase Jabber/XMPP Server - MongoDB support
 * Copyright (C) 2004-2017 "Tigase, Inc." <office@tigase.com>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, version 3 of the License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. Look for COPYING file in the top folder.
 * If not, see http://www.gnu.org/licenses/.
 */
package tigase.mongodb;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Test class to check performance of SHA-family functions
 * 
 * Below are example results
 * SHA1 1000000 for 10 in 672ms
 * SHA-256 1000000 for 10 in 731ms
 * SHA-512 1000000 for 10 in 1003ms
 * SHA1 1000000 for 20 in 515ms
 * SHA-256 1000000 for 20 in 684ms
 * SHA-512 1000000 for 20 in 981ms
 * SHA1 1000000 for 50 in 534ms
 * SHA-256 1000000 for 50 in 731ms
 * SHA-512 1000000 for 50 in 1008ms
 * SHA1 1000000 for 100 in 873ms
 * SHA-256 1000000 for 100 in 1211ms
 * SHA-512 1000000 for 100 in 1039ms
 * SHA1 1000000 for 1000 in 12566ms
 * SHA-256 1000000 for 1000 in 16233ms
 * SHA-512 1000000 for 1000 in 13838ms
 * SHA1 1000000 for 2000 in 24957ms
 * SHA-256 1000000 for 2000 in 33474ms
 * SHA-512 1000000 for 2000 in 28294ms
 * 
 * @author andrzej
 */
public class ShaTest {
	
	public static void main(String[] argc) {
		
		String[] algs = { "SHA1", "SHA-256", "SHA-512" };
		int[] sizes = { 10,	 20, 50, 100, 1000, 2000 };
		
		for (int i=0; i<sizes.length; i++) {
			for (String alg : algs) {
				test(alg, 1000000, sizes[i]);
			}
		}
		
	}
	
	public static void test(String alg, int tries, int size) {
		String test = new String();
		for (int i=0; i<size; i++) {
			test += ((char) i);
		}
		long start = System.currentTimeMillis();
		for (int i=0; i<tries; i++) {
			try {
				MessageDigest md = MessageDigest.getInstance(alg);
				byte[] result = md.digest(test.getBytes());
			} catch (NoSuchAlgorithmException ex) {
				Logger.getLogger(ShaTest.class.getName()).log(Level.SEVERE, null, ex);
			}
		}
		
		long end = System.currentTimeMillis();
		System.out.println(alg + " " + tries + " for " + size + " in " + (end-start) + "ms");
	}
}
