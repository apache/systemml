# -------------------------------------------------------------
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# -------------------------------------------------------------
import os
import unittest

import numpy as np
from systemds.context import SystemDSContext
from systemds.examples.tutorials.adult import DataManager
from systemds.operator import OperationNode, Matrix, Frame
from systemds.operator.algorithm import kmeans, multiLogReg, multiLogRegPredict, l2svm, confusionMatrix, scale, scaleApply, split, winsorize
from systemds.script_building import DMLScript


class Test_DMLScript(unittest.TestCase):
    """
    Test class for adult dml script tutorial code.
    """

    sds: SystemDSContext = None
    d: DataManager = None
    neural_net_src_path: str = "tests/examples/tutorials/neural_net_source.dml"
    preprocess_src_path: str = "tests/examples/tutorials/preprocess.dml"
    dataset_path_train: str = "../../test/resources/datasets/adult/train_data.csv"
    dataset_path_train_mtd: str = "../../test/resources/datasets/adult/train_data.csv.mtd"
    dataset_path_test: str = "../../test/resources/datasets/adult/test_data.csv"
    dataset_path_test_mtd: str = "../../test/resources/datasets/adult/test_data.csv.mtd"
    dataset_jspec: str = "../../test/resources/datasets/adult/jspec.json"

    @classmethod
    def setUpClass(cls):
        cls.sds = SystemDSContext()
        cls.d = DataManager()

    @classmethod
    def tearDownClass(cls):
        cls.sds.close()

    def test_train_data(self):
        x = self.d.get_train_data()
        self.assertEqual((32561, 14), x.shape)

    def test_train_labels(self):
        y = self.d.get_train_labels()
        self.assertEqual((32561,), y.shape)

    def test_test_data(self):
        x_l = self.d.get_test_data()
        self.assertEqual((16281, 14), x_l.shape)

    def test_test_labels(self):
        y_l = self.d.get_test_labels()
        self.assertEqual((16281,), y_l.shape)

    def test_preprocess(self):
        #assumes certain preprocessing
        train_data, train_labels, test_data, test_labels = self.d.get_preprocessed_dataset()
        self.assertEqual((30162,104), train_data.shape)
        self.assertEqual((30162, ), train_labels.shape)
        self.assertEqual((15060,104), test_data.shape)
        self.assertEqual((15060, ), test_labels.shape)

    def test_multi_log_reg(self):
        # Reduced because we want the tests to finish a bit faster.
        train_count = 15000
        test_count = 5000

        train_data, train_labels, test_data, test_labels = self.d.get_preprocessed_dataset()

        # Train data
        X = self.sds.from_numpy( train_data[:train_count])
        Y = self.sds.from_numpy( train_labels[:train_count])
        Y = Y + 1.0

        # Test data
        Xt = self.sds.from_numpy(test_data[:test_count])
        Yt = self.sds.from_numpy(test_labels[:test_count])
        Yt = Yt + 1.0

        betas = multiLogReg(X, Y)

        [_, y_pred, acc] = multiLogRegPredict(Xt, betas, Yt).compute()

        self.assertGreater(acc, 80)

        confusion_matrix_abs, _ = confusionMatrix(self.sds.from_numpy(y_pred), Yt).compute()

        self.assertTrue(
            np.allclose(
                confusion_matrix_abs,
                np.array([[3503, 503],
                          [268, 726]])
            )
        )

    def test_neural_net(self):
        # Reduced because we want the tests to finish a bit faster.
        train_count = 15000
        test_count = 5000

        train_data, train_labels, test_data, test_labels = self.d.get_preprocessed_dataset(interpolate=True, standardize=True, dimred=0.1)

        # Train data
        X = self.sds.from_numpy( train_data[:train_count])
        Y = self.sds.from_numpy( train_labels[:train_count])

        # Test data
        Xt = self.sds.from_numpy(test_data[:test_count])
        Yt = self.sds.from_numpy(test_labels[:test_count])

        FFN_package = self.sds.source(self.neural_net_src_path, "fnn", print_imported_methods=True)

        network = FFN_package.train(X, Y, 1, 16, 0.01, 1)

        self.assertTrue(type(network) is not None) # sourcing and training seems to works

        FFN_package.save_model(network, '"model/python_FFN/"').compute(verbose=True)

        # TODO This does not work yet, not sure what the problem is
        #probs = FFN_package.predict(Xt, network).compute(True)
        # FFN_package.eval(Yt, Yt).compute()



    def test_level1(self):
        # Reduced because we want the tests to finish a bit faster.
        train_count = 15000
        test_count = 5000
        train_data, train_labels, test_data, test_labels = self.d.get_preprocessed_dataset(interpolate=True,
                                                                                           standardize=True, dimred=0.1)
        # Train data
        X = self.sds.from_numpy(train_data[:train_count])
        Y = self.sds.from_numpy(train_labels[:train_count])
        Y = Y + 1.0

        # Test data
        Xt = self.sds.from_numpy(test_data[:test_count])
        Yt = self.sds.from_numpy(test_labels[:test_count])
        Yt = Yt + 1.0

        betas = multiLogReg(X, Y)

        [_, y_pred, acc] = multiLogRegPredict(Xt, betas, Yt).compute()
        self.assertGreater(acc, 80) #Todo remove?
        # todo add text how high acc should be with this config

        confusion_matrix_abs, _ = confusionMatrix(self.sds.from_numpy(y_pred), Yt).compute()
        # todo print confusion matrix? Explain cm?
        self.assertTrue(
            np.allclose(
                confusion_matrix_abs,
                np.array([[3583, 502],
                          [245, 670]])
            )
        )

    def test_level2(self):

        train_count = 32561
        test_count = 16281

        SCHEMA = '"DOUBLE,STRING,DOUBLE,STRING,DOUBLE,STRING,STRING,STRING,STRING,STRING,DOUBLE,DOUBLE,DOUBLE,STRING,STRING"'

        F1 = self.sds.read(
            self.dataset_path_train,
            schema=SCHEMA
        )
        F2 = self.sds.read(
            self.dataset_path_test,
            schema=SCHEMA
        )

        jspec = self.sds.read(self.dataset_jspec, data_type="scalar", value_type="string")
        PREPROCESS_package = self.sds.source(self.preprocess_src_path, "preprocess", print_imported_methods=True)

        X1 = F1.rbind(F2)
        X1, M1 = X1.transform_encode(spec=jspec)

        X = PREPROCESS_package.get_X(X1, 1, train_count)
        Y = PREPROCESS_package.get_Y(X1, 1, train_count)

        Xt = PREPROCESS_package.get_X(X1, train_count, train_count+test_count)
        Yt = PREPROCESS_package.get_Y(X1, train_count, train_count+test_count)

        Yt = PREPROCESS_package.replace_value(Yt, 3.0, 1.0)
        Yt = PREPROCESS_package.replace_value(Yt, 4.0, 2.0)

        # better alternative for encoding. This was intended, but it does not work
        #F2 = F2.replace("<=50K.", "<=50K")
        #F2 = F2.replace(">50K.", ">50K")
        #X1, M = F1.transform_encode(spec=jspec)
        #X2 = F2.transform_apply(spec=jspec, meta=M)

        #X = PREPROCESS_package.get_X(X1, 1, train_count)
        #Y = PREPROCESS_package.get_Y(X1, 1, train_count)
        #Xt = PREPROCESS_package.get_X(X2, 1, test_count)
        #Yt = PREPROCESS_package.get_Y(X2, 1, test_count)

        # TODO somehow throws error at predict with this included
        #X, mean, sigma = scale(X, True, True)
        #Xt = scaleApply(Xt, mean, sigma)

        betas = multiLogReg(X, Y)

        [_, y_pred, acc] = multiLogRegPredict(Xt, betas, Yt)

        confusion_matrix_abs, _ = confusionMatrix(y_pred, Yt).compute()
        print(confusion_matrix_abs)
        self.assertTrue(
            np.allclose(
                confusion_matrix_abs,
                np.array([[11593.,  1545.],
                          [842., 2302.]])
            )
        )

    def test_level3(self):
        train_count = 32561
        test_count = 16281

        SCHEMA = '"DOUBLE,STRING,DOUBLE,STRING,DOUBLE,STRING,STRING,STRING,STRING,STRING,DOUBLE,DOUBLE,DOUBLE,STRING,STRING"'

        F1 = self.sds.read(
            self.dataset_path_train,
            schema=SCHEMA
        )
        F2 = self.sds.read(
            self.dataset_path_test,
            schema=SCHEMA
        )

        jspec = self.sds.read(self.dataset_jspec, data_type="scalar", value_type="string")
        PREPROCESS_package = self.sds.source(self.preprocess_src_path, "preprocess", print_imported_methods=True)

        X1 = F1.rbind(F2)
        X1, M1 = X1.transform_encode(spec=jspec)

        X = PREPROCESS_package.get_X(X1, 1, train_count)
        Y = PREPROCESS_package.get_Y(X1, 1, train_count)

        Xt = PREPROCESS_package.get_X(X1, train_count, train_count + test_count)
        Yt = PREPROCESS_package.get_Y(X1, train_count, train_count + test_count)

        Yt = PREPROCESS_package.replace_value(Yt, 3.0, 1.0)
        Yt = PREPROCESS_package.replace_value(Yt, 4.0, 2.0)

        # better alternative for encoding
        # F2 = F2.replace("<=50K.", "<=50K")
        # F2 = F2.replace(">50K.", ">50K")
        # X1, M = F1.transform_encode(spec=jspec)
        # X2 = F2.transform_apply(spec=jspec, meta=M)

        # X = PREPROCESS_package.get_X(X1, 1, train_count)
        # Y = PREPROCESS_package.get_Y(X1, 1, train_count)
        # Xt = PREPROCESS_package.get_X(X2, 1, test_count)
        # Yt = PREPROCESS_package.get_Y(X2, 1, test_count)

        # TODO somehow throws error at predict with this included
        # X, mean, sigma = scale(X, True, True)
        # Xt = scaleApply(Xt, mean, sigma)

        FFN_package = self.sds.source(self.neural_net_src_path, "fnn", print_imported_methods=True)

        epochs = 1
        batch_size = 16
        learning_rate = 0.01
        seed = 42

        network = FFN_package.train(X, Y, epochs, batch_size, learning_rate, seed)

        """
        If more ressources are available, one can also choose to train the model using a parameter server.
        Here we use the same parameters as before, however we need to specifiy a few more.
        """
        ################################################################################################################
        # workers = 1
        # utype = '"BSP"'
        # freq = '"EPOCH"'
        # mode = '"LOCAL"'
        # network = FFN_package.train_paramserv(X, Y, epochs,
        #                                       batch_size, learning_rate, workers, utype, freq, mode,
        #                                       seed)
        ################################################################################################################

        FFN_package.save_model(network, '"model/python_FFN/"').compute(verbose=True)

        """
        Next we evaluate our network on the test set which was not used for training.
        The predict function with the test features and our trained network returns a matrix of class probabilities.
        This matrix contains for each test sample the probabilities for each class.
        For predicting the most likely class of a sample, we choose the class with the highest probability.
        """
        ################################################################################################################
        #probs = FFN_package.predict(Xt, network)
        ################################################################################################################
        """
        To evaluate how well our model performed on the test set, we can use the probability matrix from the predict call and the real test labels
        and compute the log-cosh loss.
        """
        ################################################################################################################
        #FFN_package.eval(Xt, Yt).compute(True)
        ################################################################################################################



if __name__ == "__main__":
    unittest.main(exit=False)
