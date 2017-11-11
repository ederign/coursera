package edu.coursera.concurrent;

import edu.rice.pcdp.Actor;
import edu.rice.pcdp.PCDP;

/**
 * An actor-based implementation of the Sieve of Eratosthenes.
 * <p>
 * countPrimes to determin the number of primes <= limit.
 */
public final class SieveActor extends Sieve {

    /**
     * {@inheritDoc}
     * <p>
     * TODO Use the SieveActorActor class to calculate the number of primes <=
     * limit in parallel. You might consider how you can model the Sieve of
     * Eratosthenes as a pipeline of actors, each corresponding to a single
     * prime number.
     */
    @Override
    public int countPrimes(final int limit) {

        SieveActorActor s = new SieveActorActor(2);

        PCDP.finish(() -> {
            for (int i = 3; i <= limit; i += 2) {
                s.send(i);
            }
            s.send(0);
        });

        int total = 0;
        SieveActorActor currentActor = s;
        while (currentActor != null) {
            total += currentActor.numLocalPrimes;
            currentActor = currentActor.nextActor;
        }

        return total;
    }

    /**
     * An actor class that helps implement the Sieve of Eratosthenes in
     * parallel.
     */
    public static final class SieveActorActor extends Actor {

        private static final int MAX_LOCAL_PRIMES = 1000;
        private final int localPrimes[];
        private int numLocalPrimes;
        private SieveActorActor nextActor;

        public SieveActorActor(int localPrime) {
            this.numLocalPrimes = 1;
            this.localPrimes = new int[MAX_LOCAL_PRIMES];
            this.localPrimes[0] = localPrime;
        }

        /**
         * Process a single message sent to this actor.
         * <p>
         * @param msg Received message
         */
        @Override
        public void process(final Object msg) {
            final int candidate = (Integer) msg;

            if (candidate <= 0) {
                if (nextActor != null) {
                    nextActor.send(msg);
                }
            } else {
                if (isPrime(candidate)) {
                    if (numLocalPrimes < MAX_LOCAL_PRIMES) {
                        localPrimes[numLocalPrimes] = candidate;
                        numLocalPrimes++;
                    } else if (nextActor == null) {
                        nextActor = new SieveActorActor(candidate);
                    } else {
                        nextActor.send(candidate);
                    }
                }
            }
        }

        private boolean isPrime(final int candidate) {
            for (int i = 0; i < numLocalPrimes; i++) {
                if (candidate % localPrimes[i] == 0) {
                    return false;
                }
            }
            return true;
        }
    }
}
