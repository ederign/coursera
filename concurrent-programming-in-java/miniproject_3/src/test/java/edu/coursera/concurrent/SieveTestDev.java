package edu.coursera.concurrent;

import junit.framework.TestCase;

public class SieveTestDev extends TestCase {

    public void test1() {
        SieveActor s = new SieveActor();
        assertEquals(4,
                     s.countPrimes(10));
    }
}
