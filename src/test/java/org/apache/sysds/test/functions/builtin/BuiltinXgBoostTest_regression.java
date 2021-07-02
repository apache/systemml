package org.apache.sysds.test.functions.builtin;

import org.apache.sysds.common.Types;
import org.apache.sysds.runtime.matrix.data.MatrixValue;
import org.apache.sysds.test.AutomatedTestBase;
import org.apache.sysds.test.TestConfiguration;
import org.apache.sysds.test.TestUtils;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;

public class BuiltinXgBoostTest_regression extends AutomatedTestBase {
    private final static String TEST_NAME = "xgboost_regression";
    private final static String TEST_DIR = "functions/builtin/";
    private static final String TEST_CLASS_DIR = TEST_DIR + BuiltinXgBoostTest_regression.class.getSimpleName() + "/";
    double eps = 1e-10;

    @Override
    public void setUp() {
        TestUtils.clearAssertionInformation();
        addTestConfiguration(TEST_NAME, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME, new String[]{"C"}));
    }

    @Parameterized.Parameter()
    public int rows;
    @Parameterized.Parameter(1)
    public int cols;
    @Parameterized.Parameter(2)
    public int sml_type;
    @Parameterized.Parameter(3)
    public int num_trees;
    @Parameterized.Parameter(4)
    public double learning_rate;
    @Parameterized.Parameter(5)
    public int max_depth;
    @Parameterized.Parameter(6)
    public double lambda;

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {8, 2, 1, 2, 0.3, 6, 1.0},
        });
    }

    @Test
    public void testXgBoost() {
        executeXgBoost(Types.ExecMode.SINGLE_NODE);
    }

    private void executeXgBoost(Types.ExecMode mode) {
        Types.ExecMode platformOld = setExecMode(mode);
        try {
            loadTestConfiguration(getTestConfiguration(TEST_NAME));

            String HOME = SCRIPT_DIR + TEST_DIR;
            fullDMLScriptName = HOME + TEST_NAME + ".dml";
            programArgs = new String[]{"-args", input("X"), input("y"), input("R"), String.valueOf(sml_type),
                    String.valueOf(num_trees), output("M")};

            double[][] y = {
                    {5.0},
                    {1.9},
                    {10.0},
                    {8.0},
                    {0.7}};

            double[][] X = {
                    {4.5, 4.0, 3.0, 2.8, 3.5},
                    {1.9, 2.4, 1.0, 3.4, 2.9},
                    {2.0, 1.1, 1.0, 4.9, 3.4},
                    {2.3, 5.0, 2.0, 1.4, 1.8},
                    {2.1, 1.1, 3.0, 1.0, 1.9}};

            double[][] R = {
                    {1.0, 1.0, 1.0, 1.0, 1.0}};


            writeInputMatrixWithMTD("X", X, true);
            writeInputMatrixWithMTD("y", y, true);
            writeInputMatrixWithMTD("R", R, true);

            runTest(true, false, null, -1);

            HashMap<MatrixValue.CellIndex, Double> actual_M = readDMLMatrixFromOutputDir("M");

            // root node of first tree
            TestUtils.compareScalars(actual_M.get(new MatrixValue.CellIndex(1,2)), 1.0, eps);
            TestUtils.compareScalars(actual_M.get(new MatrixValue.CellIndex(2,2)), 1.0, eps);
            TestUtils.compareScalars(actual_M.get(new MatrixValue.CellIndex(3,2)), 1.0, eps);
            TestUtils.compareScalars(actual_M.get(new MatrixValue.CellIndex(4,2)), 4.0, eps);
            TestUtils.compareScalars(actual_M.get(new MatrixValue.CellIndex(5,2)), 1.0, eps);
            TestUtils.compareScalars(actual_M.get(new MatrixValue.CellIndex(6,2)), 2.10, eps);

            // random leaf node of first tree
            TestUtils.compareScalars(actual_M.get(new MatrixValue.CellIndex(1,8)), 7.0, eps);
            TestUtils.compareScalars(actual_M.get(new MatrixValue.CellIndex(2,8)), 1.0, eps);
            TestUtils.compareScalars(String.valueOf(actual_M.get(new MatrixValue.CellIndex(3, 8))), "null");
            TestUtils.compareScalars(String.valueOf(actual_M.get(new MatrixValue.CellIndex(4, 8))), "null");
            TestUtils.compareScalars(String.valueOf(actual_M.get(new MatrixValue.CellIndex(5, 8))), "null");
            TestUtils.compareScalars(actual_M.get(new MatrixValue.CellIndex(6,8)), 5.0, eps);

            // last node in first tree
            TestUtils.compareScalars(actual_M.get(new MatrixValue.CellIndex(1,10)), 13.0, eps);
            TestUtils.compareScalars(actual_M.get(new MatrixValue.CellIndex(2,10)), 1.0, eps);
            TestUtils.compareScalars(String.valueOf(actual_M.get(new MatrixValue.CellIndex(3, 10))), "null");
            TestUtils.compareScalars(String.valueOf(actual_M.get(new MatrixValue.CellIndex(4, 10))), "null");
            TestUtils.compareScalars(String.valueOf(actual_M.get(new MatrixValue.CellIndex(5, 10))), "null");
            TestUtils.compareScalars(String.valueOf(actual_M.get(new MatrixValue.CellIndex(6, 10))), "null");

            // root node of second tree
            TestUtils.compareScalars(actual_M.get(new MatrixValue.CellIndex(1,11)), 1.0, eps);
            TestUtils.compareScalars(actual_M.get(new MatrixValue.CellIndex(2,11)), 2.0, eps);
            TestUtils.compareScalars(actual_M.get(new MatrixValue.CellIndex(3,11)), 1.0, eps);
            TestUtils.compareScalars(actual_M.get(new MatrixValue.CellIndex(4,11)), 4.0, eps);
            TestUtils.compareScalars(actual_M.get(new MatrixValue.CellIndex(5,11)), 1.0, eps);
            TestUtils.compareScalars(actual_M.get(new MatrixValue.CellIndex(6,11)), 2.10, eps);

            // random leaf node of second tree
            TestUtils.compareScalars(actual_M.get(new MatrixValue.CellIndex(1,15)), 5.0, eps);
            TestUtils.compareScalars(actual_M.get(new MatrixValue.CellIndex(2,15)), 2.0, eps);
            TestUtils.compareScalars(String.valueOf(actual_M.get(new MatrixValue.CellIndex(3, 15))), "null");
            TestUtils.compareScalars(String.valueOf(actual_M.get(new MatrixValue.CellIndex(4, 15))), "null");
            TestUtils.compareScalars(String.valueOf(actual_M.get(new MatrixValue.CellIndex(5, 15))), "null");
            TestUtils.compareScalars(actual_M.get(new MatrixValue.CellIndex(6,15)), 2.10, eps);

            // last node in matrix and second tree
            TestUtils.compareScalars(actual_M.get(new MatrixValue.CellIndex(1,19)), 13.0, eps);
            TestUtils.compareScalars(actual_M.get(new MatrixValue.CellIndex(2,19)), 2.0, eps);
            TestUtils.compareScalars(String.valueOf(actual_M.get(new MatrixValue.CellIndex(3, 19))), "null");
            TestUtils.compareScalars(String.valueOf(actual_M.get(new MatrixValue.CellIndex(4, 19))), "null");
            TestUtils.compareScalars(String.valueOf(actual_M.get(new MatrixValue.CellIndex(5, 19))), "null");
            TestUtils.compareScalars(String.valueOf(actual_M.get(new MatrixValue.CellIndex(6, 19))), "null");


        } catch (Exception ex) {
            System.out.println("[ERROR] Xgboost test failed, cause: " + ex);
            throw ex;
        } finally {
            rtplatform = platformOld;
        }
    }
}
