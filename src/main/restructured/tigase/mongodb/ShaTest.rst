.. java:import:: java.security MessageDigest

.. java:import:: java.security NoSuchAlgorithmException

.. java:import:: java.util.logging Level

.. java:import:: java.util.logging Logger

ShaTest
=======

.. java:package:: tigase.mongodb
   :noindex:

.. java:type:: public class ShaTest

   Test class to check performance of SHA-family functions

   Below are example results SHA1 1000000 for 10 in 672ms SHA-256 1000000 for 10 in 731ms SHA-512 1000000 for 10 in 1003ms SHA1 1000000 for 20 in 515ms SHA-256 1000000 for 20 in 684ms SHA-512 1000000 for 20 in 981ms SHA1 1000000 for 50 in 534ms SHA-256 1000000 for 50 in 731ms SHA-512 1000000 for 50 in 1008ms SHA1 1000000 for 100 in 873ms SHA-256 1000000 for 100 in 1211ms SHA-512 1000000 for 100 in 1039ms SHA1 1000000 for 1000 in 12566ms SHA-256 1000000 for 1000 in 16233ms SHA-512 1000000 for 1000 in 13838ms SHA1 1000000 for 2000 in 24957ms SHA-256 1000000 for 2000 in 33474ms SHA-512 1000000 for 2000 in 28294ms

   :author: andrzej

Methods
-------
main
^^^^

.. java:method:: public static void main(String[] argc)
   :outertype: ShaTest

test
^^^^

.. java:method:: public static void test(String alg, int tries, int size)
   :outertype: ShaTest

