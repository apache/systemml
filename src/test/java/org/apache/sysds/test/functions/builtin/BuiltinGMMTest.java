/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.sysds.test.functions.builtin;

import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.sysds.common.Types;
import org.apache.sysds.lops.LopProperties;
import org.apache.sysds.runtime.matrix.data.MatrixValue;
import org.apache.sysds.test.AutomatedTestBase;
import org.apache.sysds.test.TestConfiguration;
import org.apache.sysds.test.TestUtils;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Random;
import java.util.stream.Stream;

public class BuiltinGMMTest extends AutomatedTestBase {
	private final static String TEST_NAME = "GMM";
	private final static String TEST_DIR = "functions/builtin/";
	private static final String TEST_CLASS_DIR = TEST_DIR + BuiltinGMMTest.class.getSimpleName() + "/";

	private final static double eps = 1;
	private final static int rows = 100;
	private final static double spDense = 0.99;

	@Override
	public void setUp() {
		addTestConfiguration(TEST_NAME,new TestConfiguration(TEST_CLASS_DIR, TEST_NAME,new String[]{"B"}));
	}

	@Test
	public void testGMMCP() {
		runGMMTest(3,3,"VVI", 1,LopProperties.ExecType.CP);
	}

	@Test
	public void testGMMManual() {
		runGMMTest(3,3,"VVI",2, LopProperties.ExecType.CP);
	}


	private void runGMMTest(int G_mixtures, int iter, String model, int test,  LopProperties.ExecType instType)
	{
		Types.ExecMode platformOld = setExecMode(instType);

		try
		{
			loadTestConfiguration(getTestConfiguration(TEST_NAME));
			String HOME = SCRIPT_DIR + TEST_DIR;
			fullDMLScriptName = HOME + TEST_NAME + ".dml";
			programArgs = new String[]{ "-args", input("A"), String.valueOf(G_mixtures), String.valueOf(iter), model, String.valueOf("0.00000001"),output("B") };

			fullRScriptName = HOME + TEST_NAME + ".R";
			rCmd = "Rscript" + " " + fullRScriptName + " " + inputDir()+ " " + model + " "  +String.valueOf(G_mixtures)+ " "+ expectedDir();

			if(test ==1 ) {
				//generate actual dataset
				double[][] X = getRandomMatrix(rows, 2, 0.5, 1, 1.0, 714);
				writeInputMatrixWithMTD("A", X, true);
			}
			if(test == 2)
			{
				Point[]centers = {new Point(2,3),new Point(5,10),new Point(10,6)}; // distance {p1,p2} = 7.6,  {p2,p3} = 6.4 and {p1,p3} = 8.5
				//generate points in Radius R = 0.3
				Point[] points1 = getPoints(centers[0], 5, 20);
				Point[] points2 = getPoints(centers[1], 5, 20);
				Point[] points3 = getPoints(centers[2], 5, 20);
				Stream<Point> stream = Stream.of();
				stream = Stream.concat(stream, Arrays.stream(points1));
				stream = Stream.concat(stream, Arrays.stream(points2));
				stream = Stream.concat(stream, Arrays.stream(points3));
				Point[] allPoint = stream.toArray(Point[]::new);
				int rows = points1.length+points2.length+points3.length;
				double[][] A = new double[rows][2];
				for(int i =0; i< A.length; i++)
				{
					A[i][0] = allPoint[i].getX();
					A[i][0] = allPoint[i].getY();
					A[i][0] = allPoint[i].getX();
					A[i][1] = allPoint[i].getY();
				}
				A[1][0] = 7.9;
				A[1][1] = 8.5;
				A[0][0] = 30;
				A[0][1] = 30;
//				A[1][0] = 200;
//				A[1][1] = 200;
				for(int i =0; i< A.length; i++)
					System.out.println(A[i][0]+","+A[i][1]);
				writeInputMatrixWithMTD("A", A, true);
			}

			runTest(true, false, null, -1);
			runRScript(true);

			//compare matrices
			HashMap<MatrixValue.CellIndex, Double> dmlfile = readDMLMatrixFromHDFS("B");
			HashMap<MatrixValue.CellIndex, Double> rfile  = readRMatrixFromFS("B");
			TestUtils.compareMatrices(dmlfile, rfile, eps, "Stat-DML", "Stat-R");
		}
		finally {
			rtplatform = platformOld;
		}
	}

	private Point[] getPoints(Point p, double R, int numPoint){
		Point[] pArray = new Point[numPoint];

//		double x = 0, y = 0
		for(int i=0; i<numPoint; i++ ) {
			double x = Double.NaN, y = Double.NaN;
			while(Double.isNaN(x) || Double.isNaN(y)) {
				x = getGaussian(p.getX(), 1);
				y = getGaussian(p.getY(), 1);
			}
			pArray[i] = new Point(x, y);
		}
			// If you need it in Cartesian coordinates
//		double x = r * Math.cos(a);
//		double y = r * Math.sin(a);
		return pArray;
	}
	private double getGaussian(double aMean, double aVariance){
		Random r = new Random();
		return aMean + r.nextGaussian() * aVariance;
	}
}

class Point {
	private double x;
	private double y;
	Point(double p1, double p2)
	{
		x = p1;
		y = p2;
	}

	public double getX() {
		return x;
	}

	public void setX(double x) {
		this.x = x;
	}

	public double getY() {
		return y;
	}

	public void setY(double y) {
		this.y = y;
	}
}

