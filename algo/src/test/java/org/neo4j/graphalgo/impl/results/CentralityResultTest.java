package org.neo4j.graphalgo.impl.results;

import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.neo4j.graphalgo.Normalization;
import org.neo4j.graphalgo.core.write.Exporter;
import org.neo4j.graphalgo.core.write.Translators;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class CentralityResultTest {
    @Test
    public void doubleArrayResult() {
        DoubleArrayResult result = new DoubleArrayResult(new double[] {1,2,3,4});

        assertEquals(4.0, result.computeMax(), 0.01);
        assertEquals(10.0, result.computeL1Norm(), 0.01);
        assertEquals(5.477225575051661, result.computeL2Norm(), 0.01);
    }


    @Test
    public void doubleArrayResultExport() {
        String property = "eigenvector";
        DoubleArrayResult result = new DoubleArrayResult(new double[] {1,2,3,4});

        Exporter exporter = mock(Exporter.class);
        Normalization.MAX.apply(result).export(property, exporter);

        verify(exporter).write(property, new double[] {0.25,0.5,0.75,1.0}, Translators.DOUBLE_ARRAY_TRANSLATOR);
    }

    @Test
    public void partitionedPrimitiveDoubleArrayResult() {
        double[][] partitions = new double[][] { {1.0,2.0}, {3.0,4.0} };
        int[] starts = new int[] { 0, 2};
        PartitionedPrimitiveDoubleArrayResult result = new PartitionedPrimitiveDoubleArrayResult(partitions, starts);

        assertEquals(4.0, result.computeMax(), 0.01);
        assertEquals(10.0, result.computeL1Norm(), 0.01);
        assertEquals(5.477225575051661, result.computeL2Norm(), 0.01);
    }

    @Test
    public void partitionedPrimitiveDoubleArrayResultExport() {
        String property = "eigenvector";
        double[][] partitions = new double[][] { {1.0,2.0}, {3.0,4.0} };
        int[] starts = new int[] { 0, 2};
        PartitionedPrimitiveDoubleArrayResult result = new PartitionedPrimitiveDoubleArrayResult(partitions, starts);

        Exporter exporter = mock(Exporter.class);
        Normalization.MAX.apply(result).export(property, exporter);

        verify(exporter).write(eq(property), argThat(arrayEq(new double[][] { {0.25, 0.5}, {0.75, 1.0} })), eq(result));
    }


    @Test
    public void partitionedDoubleArrayResult() {
        double[][] partitions = new double[][] { {1.0,2.0}, {3.0,4.0} };
        long[] starts = new long[] { 0, 2};
        PartitionedDoubleArrayResult result = new PartitionedDoubleArrayResult(partitions, starts);

        assertEquals(4.0, result.computeMax(), 0.01);
        assertEquals(10.0, result.computeL1Norm(), 0.01);
        assertEquals(5.477225575051661, result.computeL2Norm(), 0.01);
    }

    @Test
    public void partitionedDoubleArrayResultExport() {
        String property = "eigenvector";
        double[][] partitions = new double[][] { {1.0,2.0}, {3.0,4.0} };
        long[] starts = new long[] { 0, 2};
        PartitionedDoubleArrayResult result = new PartitionedDoubleArrayResult(partitions, starts);

        Exporter exporter = mock(Exporter.class);
        Normalization.MAX.apply(result).export(property, exporter);

        verify(exporter).write(eq(property), argThat(arrayEq(new double[][] { {0.25, 0.5}, {0.75, 1.0} })), eq(result));
    }


    private ArrayMatcher arrayEq(double[][] expected) {
        return new ArrayMatcher(expected);
    }


    class ArrayMatcher implements ArgumentMatcher<double[][]> {
        private double[][] expected;

        public ArrayMatcher(double[][] expected) {
            this.expected = expected;
        }

        @Override
        public boolean matches(double[][] actual) {
            return Arrays.deepEquals(expected, actual);
        }
    }
}
