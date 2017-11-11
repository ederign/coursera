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
                s.process(i);
            }
        });

        int total = 0;
        SieveActorActor currentActor = s;
        while (currentActor != null) {
            total += currentActor.actorLocalNumberOfPrimes;
            currentActor = currentActor.next;
        }

        return total;
    }

    /**
     * An actor class that helps implement the Sieve of Eratosthenes in
     * parallel.
     */
    public static final class SieveActorActor extends Actor {

        private static final int MAX_LOCAL_PRIMES = 10_000;
        private final int localPrimes[];
        private int actorLocalNumberOfPrimes;
        private SieveActorActor next;

        public SieveActorActor(int localPrime) {
            this.actorLocalNumberOfPrimes = 1;
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
            if (isLocalPrime(candidate)) {
                if (actorLocalNumberOfPrimes < MAX_LOCAL_PRIMES) {
                    localPrimes[actorLocalNumberOfPrimes] = candidate;
                    actorLocalNumberOfPrimes++;
                } else if (next == null) {
                    next = new SieveActorActor(candidate);
                } else {
                    next.send(candidate);
                }
            }
        }

        private boolean isLocalPrime(int candidate) {
            for (int i = 0; i < actorLocalNumberOfPrimes; i++) {
                int currentLocalPrime = localPrimes[i];
                if (candidate % currentLocalPrime == 0) {
                    return false;
                }
            }
            return true;
        }
    }
}
