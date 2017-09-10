package edu.coursera.parallel;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;

/**
 * Class wrapping methods for implementing reciprocal array sum in parallel.
 */
public final class ReciprocalArraySum {

    /**
     * Default constructor.
     */
    private ReciprocalArraySum() {
    }

    /**
     * Sequentially compute the sum of the reciprocal values for a given array.
     * @param input Input array
     * @return The sum of the reciprocals of the array input
     */
    protected static double seqArraySum(final double[] input) {
        double sum = 0;

        // Compute sum of reciprocals of array elements
        for (int i = 0; i < input.length; i++) {
            sum += 1 / input[i];
        }

        return sum;
    }

    /**
     * Computes the size of each chunk, given the number of chunks to create
     * across a given number of elements.
     * @param nChunks The number of chunks to create
     * @param nElements The number of elements to chunk across
     * @return The default chunk size
     */
    private static int getChunkSize(final int nChunks,
                                    final int nElements) {
        // Integer ceil
        return (nElements + nChunks - 1) / nChunks;
    }

    /**
     * Computes the inclusive element index that the provided chunk starts at,
     * given there are a certain number of chunks.
     * @param chunk The chunk to compute the start of
     * @param nChunks The number of chunks created
     * @param nElements The number of elements to chunk across
     * @return The inclusive index that this chunk starts at in the set of
     * nElements
     */
    private static int getChunkStartInclusive(final int chunk,
                                              final int nChunks,
                                              final int nElements) {
        final int chunkSize = getChunkSize(nChunks,
                                           nElements);
        return chunk * chunkSize;
    }

    /**
     * Computes the exclusive element index that the provided chunk ends at,
     * given there are a certain number of chunks.
     * @param chunk The chunk to compute the end of
     * @param nChunks The number of chunks created
     * @param nElements The number of elements to chunk across
     * @return The exclusive end index for this chunk
     */
    private static int getChunkEndExclusive(final int chunk,
                                            final int nChunks,
                                            final int nElements) {
        final int chunkSize = getChunkSize(nChunks,
                                           nElements);
        final int end = (chunk + 1) * chunkSize;
        if (end > nElements) {
            return nElements;
        } else {
            return end;
        }
    }

    /**
     * This class stub can be filled in to implement the body of each task
     * created to perform reciprocal array sum in parallel.
     */
    private static class ReciprocalArraySumTask extends RecursiveAction {

        static int SEQUENTIAL_THRESHOLD = 5000;

        /**
         * Starting index for traversal done by this task.
         */
        private final int startIndexInclusive;
        /**
         * Ending index for traversal done by this task.
         */
        private final int endIndexExclusive;
        /**
         * Input array to reciprocal sum.
         */
        private final double[] input;
        /**
         * Intermediate value produced by this task.
         */
        private double value;

        private int numTasks;
        private int numElements;

        /**
         * Constructor.
         * @param setStartIndexInclusive Set the starting index to begin
         * parallel traversal at.
         * @param setEndIndexExclusive Set ending index for parallel traversal.
         * @param setInput Input values
         */
        ReciprocalArraySumTask(final int setStartIndexInclusive,
                               final int setEndIndexExclusive,
                               final double[] setInput) {
            this.startIndexInclusive = setStartIndexInclusive;
            this.endIndexExclusive = setEndIndexExclusive;
            this.input = setInput;
            this.numTasks = 2;
            this.numElements = input.length;
        }

        public ReciprocalArraySumTask(final int setStartIndexInclusive,
                                      final int setEndIndexExclusive,
                                      final double[] setInput,
                                      int numTasks,
                                      int numElements) {
            this.startIndexInclusive = setStartIndexInclusive;
            this.endIndexExclusive = setEndIndexExclusive;
            this.input = setInput;
            this.numTasks = numTasks;
            this.numElements = numElements;
        }

        /**
         * Getter for the value produced by this task.
         * @return Value produced by this task
         */
        public double getValue() {
            return value;
        }

        @Override
        protected void compute() {

            if ((endIndexExclusive - startIndexInclusive) <= SEQUENTIAL_THRESHOLD) {
                //seq
                for (int k = startIndexInclusive; k < endIndexExclusive; k++) {
                    value += 1 / input[k];
                }
            } else {

                List<ReciprocalArraySumTask> tasks = new ArrayList<>();
                for (int i = 0; i < numTasks; i++) {

                    int start = getChunkStartInclusive(i,
                                                       numTasks,
                                                        numElements) + startIndexInclusive;
                    int end = getChunkEndExclusive(i,
                                                   numTasks,
                                                    numElements) + startIndexInclusive;

                    ReciprocalArraySumTask t1 = new ReciprocalArraySumTask(start,
                                                                           end,
                                                                           input,
                                                                           numTasks,
                                                                           end - start);

                    if(i < numTasks - 1){
                        t1.fork();
                        tasks.add(t1);
                    }
                    else{
                         t1.compute();
                        value += t1.getValue();
                    }
                }
                for (ReciprocalArraySumTask task : tasks) {
                    task.join();
                    value += task.getValue();
                }




//                int start1 = getChunkStartInclusive(0,
//                                                    3,
//                                                    numElements) + startIndexInclusive;
//                int end1 = getChunkEndExclusive(0,
//                                                3,
//                                                numElements) + startIndexInclusive;
//                int start2 = getChunkStartInclusive(1,
//                                                    3,
//                                                    numElements) + startIndexInclusive;
//                int end2 = getChunkEndExclusive(1,
//                                                3,
//                                                numElements) + startIndexInclusive;
//
//                int start3 = getChunkStartInclusive(2,
//                                                    3,
//                                                    numElements) + startIndexInclusive;
//                int end3 = getChunkEndExclusive(2,
//                                                3,
//                                                numElements) + startIndexInclusive;
//
//                ReciprocalArraySumTask t1 = new ReciprocalArraySumTask(start1,
//                                                                       end1,
//                                                                       input,
//                                                                       numTasks,
//                                                                       end1 - start1);
//
//                ReciprocalArraySumTask t2 = new ReciprocalArraySumTask(start2,
//                                                                       end2,
//                                                                       input,
//                                                                       numTasks,
//                                                                       end2 - start2);
//                ReciprocalArraySumTask t3 = new ReciprocalArraySumTask(start3,
//                                                                       end3,
//                                                                       input,
//                                                                       numTasks,
//                                                                       end3 - start3);


//                t1.fork(); //async
//                t2.fork();
//                t3.compute();
//                t1.join();
//                t2.join();
//                value = t1.getValue() + t2.getValue() + t3.getValue();
            }
        }
    }

    public static void main(String[] args) {
        double[] input = {0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9};
        System.out.println(seqArraySum(input));

        System.out.println(parArraySum(input));
    }

    /**
     * TODO: Modify this method to compute the same reciprocal sum as
     * seqArraySum, but use two tasks running in parallel under the Java Fork
     * Join framework. You may assume that the length of the input array is
     * evenly divisible by 2.
     * @param input Input array
     * @return The sum of the reciprocals of the array input
     */
    protected static double parArraySum(final double[] input) {
        assert input.length % 2 == 0;

        ReciprocalArraySumTask r1 = new ReciprocalArraySumTask(0,
                                                               input.length,
                                                               input);

        ForkJoinPool.commonPool().invoke(r1);

        return r1.getValue();
    }

    /**
     * TODO: Extend the work you did to implement parArraySum to use a set
     * number of tasks to compute the reciprocal array sum. You may find the
     * above utilities getChunkStartInclusive and getChunkEndExclusive helpful
     * in computing the range of element indices that belong to each chunk.
     * @param input Input array
     * @param numTasks The number of tasks to create
     * @return The sum of the reciprocals of the array input
     */
    protected static double parManyTaskArraySum(final double[] input,
                                                final int numTasks) {
        ReciprocalArraySumTask r1 = new ReciprocalArraySumTask(0,
                                                               input.length,
                                                               input,
                                                               numTasks,
                                                               input.length);

        ForkJoinPool.commonPool().invoke(r1);

        return r1.getValue();
    }
}
