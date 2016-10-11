#!/usr/bin/env python
# -*- coding: utf-8 -*-
#-----------------------------------------------------------------------------
# Copyright (c) 2015, IBM Corp.
# All rights reserved.
#
# Distributed under the terms of the BSD Simplified License.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

"""
In-Database Naive Bayes modelization and prediction.
Copies the interface of sklearn.naive_bayes
"""
from __future__ import print_function
from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import
from builtins import dict
from builtins import str
from future import standard_library
standard_library.install_aliases()

from lazy import lazy
import ibmdbpy
from ibmdbpy.exceptions import IdaNaiveBayesError
import six

class NaiveBayes(object):
    """
    The Naive Bayes classification algorithm is a probabilistic classifier.
    It is based on probability models that incorporate strong independence
    assumptions. Often, the independence assumptions do not have an impact
    on reality. Therefore, they are considered naive.

    The NaiveBayes class provides an interface for using the NAIVEBAYES
    and PREDICT_NAIVEBAYES IDAX methods of dashDB/DB2.
    """

    def __init__(self, modelname = None, disc = None, bins = None):
        """
        Constructor for NaiveBayes model objects

        Parameters
        ----------
        modelname : str, optional
            The name of the Naive Bayes model that will be built. If no name is 
            specified, it will be generated automatically. If the parameter 
            corresponds to an existing model in the database, it is replaced 
            during the fitting step.


        disc : str, optional, default: ew
            Determine the automatic discretization of all continuous attributes. 
            The following values are allowed: ef, em, ew, and ewn. 
                * disc=ef
                    Equal-frequency discretization.
                    An unsupervised discretization algorithm that uses the equal
                    frequency criterion for interval bound setting.
                * disc=em
                    Minimal entropy discretization.
                    An unsupervised discretization algorithm that uses the minimal
                    entropy criterion for interval bound setting.
                * disc=ew (default)
                    Equal-width discretization.
                    An unsupervised discretization algorithm that uses the equal
                    width criterion for interval bound setting.
                * disc=ewn
                    Equal-width discretization with nice bucket limits.
                    An unsupervised discretization algorithm that uses the equal
                    width criterion for interval bound setting.

        bins : int, optional, default : 10
            Number of bins for numeric columns.
            
        Attributes
        ----------
        TODO

        Returns
        -------
            The NaiveBayes object, ready to be used for fitting and prediction.

        Examples
        --------
        >>> idadb = IdaDataBase("BLUDB-TEST")
        >>> idadf = IdaDataFrame(idadb, "IRIS")
        >>> bayes = NaiveBayes("NAIVEBAYES_TEST")
        >>> bayes.fit(idadf, column_id="ID", target="species")
        >>> bayes.predict(idadf, outtable="IRIS_PREDICTION", outtableProb="IRIS_PREDICTIONPROB")

        Notes
        -----
            Inner parameters of the model can be printed and modified by using 
            get_params and set_params. But we recommend creating a new 
            NaiveBayes model instead of modifying an existing model.
        """
        # Get set at fit step
        self._idadf = None
        self._idadb = None
        self._column_id = None
        self.target = None
        self.incolumn = None
        self.coldeftype = None
        self.coldefrole = None
        self.colpropertiestable = None

        # Get set at predict step
        self.outtable = None
        self.outtableProb = None
        self.mestimation = None

        self.modelname = modelname
        self.disc = disc
        self.bins = bins


    @lazy
    def labels_(self):
        """
        Return the labels of the classification if available
        """
        try:
            return self.predict(self._idadf, self._column_id)
        except:
            raise AttributeError(str(self.__class__) + " object has no attribute 'labels_'")

    def get_params(self):
        """
        Return the parameters of the Naive Byes model.
        """
        print(dir(self))
        params = dict()
        params['modelname'] = self.modelname
        params['disc'] = self.disc
        params['bins'] = self.bins

        params['target'] = self.target
        params['incolumn'] = self.incolumn
        params['coldeftype'] = self.coldeftype
        params['coldefrole'] = self.coldefrole
        params['colpropertiestable'] = self.colpropertiestable

        params['outtable'] = self.outtable
        params['outtableProb'] = self.outtableProb
        params['mestimation'] = self.mestimation

        return params

    def set_params(self, **params):
        """
        Modify the parameters of the Naive Bayes model.
        """
        if not params:
            # Simple optimisation to gain speed (inspect is slow)
            return self
        valid_params = self.get_params()
        for key, value in six.iteritems(params):
            if key not in valid_params:
                raise ValueError('Invalid parameter %s for estimator %s' %
                                     (key, self.__class__.__name__))
            setattr(self, key, value)
        return self

    def fit(self, idadf, target, column_id="ID", incolumn=None, coldeftype=None,
            coldefrole=None, colpropertiestable=None, verbose=False):
        """
        Create a Naive Bayes model from an IdaDataFrame.

        Parameters
        ----------
        idadf : IdaDataFrame
            The IdaDataFrame to be used as input.

        target : str
            The column of the input table that represents the class

        column_id : str, default: "ID
            The column of the input table that identifies the transaction ID.

        incolumn : str, optional
            The columns of the input table that have specific properties,
            which are separated by a semi-colon (;). Each column is succeeded
            by one or more of the following properties:
                * By type nominal (':nom') or by type continuous (':cont'). By default, numerical types are continuous, and all other types are nominal.
                * By role ':id', ':target', ':input', or ':ignore'.

            If this parameter is not specified, all columns of the input table have default properties.

        coldeftype : str, optional
            The default type of the input table columns.
            The following values are allowed: 'nom' and 'cont'.
            If the parameter is not specified, numeric columns are continuous,
            and all other columns are nominal.

        coldefrole : str, optional
            The default role of the input table columns.
            The following values are allowed: 'input' and 'ignore'.
            If the parameter is not specified, all columns are input columns.

        colpropertiestable : str, optional
            The input table where the properties of the columns of the input table are stored.
            If this parameter is not specified, the column properties of the input table
            column properties are detected automatically.

        verbose : bool, default: False
            Verbosity mode.
        """

        # Some basic checks
        if not isinstance(idadf, ibmdbpy.IdaDataFrame):
            raise TypeError("Argument should be an IdaDataFrame")
        if target not in idadf.columns:
            raise ValueError("Target is not a column in " + idadf.name)

        idadf._idadb._check_procedure("NAIVEBAYES", "Naive Bayes")

        # Check the ID
        if column_id not in idadf.columns:
            raise ValueError("No id columns is available in IdaDataFrame:" + column_id +
                             ". Either create a new ID column using add_column_id function" +
                             " or give the name of a column that can be used as ID")

        self._idadb = idadf._idadb
        self._idadf = idadf
        self._column_id = column_id

        self.target = target
        self.incolumn = incolumn
        self.coldeftype = coldeftype
        self.coldefrole = coldefrole
        self.colpropertiestable = colpropertiestable

        # Check or create a model name, drop it if it already exists.
        if self.modelname is None:
            self.modelname = idadf._idadb._get_valid_modelname('NAIVEBAYES_')
        else:
            self.modelname = ibmdbpy.utils.check_tablename(self.modelname)
            if idadf._idadb.exists_model(self.modelname):
                idadf._idadb.drop_model(self.modelname)

        # Create a temporay view
        # TODO: Why do we need actually to create a view ?
        idadf.internal_state._create_view()
        tmp_view_name = idadf.internal_state.current_state
        
        if "." in tmp_view_name:
            tmp_view_name = tmp_view_name.split('.')[-1]

        try:
            idadf._idadb._call_stored_procedure("IDAX.NAIVEBAYES ",
                                                 model = self.modelname,
                                                 intable = tmp_view_name,
                                                 id = self._column_id,
                                                 target = self.target,
                                                 incolumn = self.incolumn,
                                                 coldeftype = self.coldeftype,
                                                 coldefrole = self.coldefrole,
                                                 colPropertiesTable = self.colpropertiestable,
                                                 disc = self.disc,
                                                 bins = self.bins)
        except:
            raise
        finally:
            idadf.internal_state._delete_view()
            idadf.commit()

        self._retrieve_NaiveBayes_Model(self.modelname, verbose)

        if verbose is True:
            self.describe()

        return

    def predict(self, idadf, column_id=None, outtable=None, outtableProb=None,
                mestimation = False):
        """
        Use the Naive Bayes predict stored procedure to apply a Naive Bayes model
        to generate classification predictions for a data set.

        Parameters
        ----------
        idadf : IdaDataFrame
             IdaDataFrame to be used as input.

        column_id : str, optional
            The column of the input table that identifies a unique instance ID.
            By default, the same id column that is specified in the stored
            procedure to build the model.

        outtable : str, optional
            The name of the output table where the predictions are stored. If
            this parameter is not specified, it is generated automatically. If
            the parameter corresponds to an existing table in the database, it
            will be replaced.

        outtableProb : str, optional
            The output table where the probabilities for each of the classes are stored.
            If this parameter is not specified, the table is not created. If
            the parameter corresponds to an existing table in the database, it
            will be replaced.

        mestimation : flag, default: False
            A flag that indicates the use of m-estimation for probabilities.
            This kind of estimation might be slower than other ones, but it
            might produce better results for small or unbalanced data sets.

        Returns
        -------
        IdaDataFrame
            IdaDataFrame containing the classification decision for each
            datapoints referenced by their ID.
        """
        if not isinstance(idadf, ibmdbpy.IdaDataFrame):
            raise TypeError("Argument should be an IdaDataFrame")

        idadf._idadb._check_procedure("PREDICT_NAIVEBAYES", "Prediction for Naive Bayes")

        # Check the ID
        if column_id is None :
            column_id = self._column_id
        if column_id not in idadf.columns:
            raise ValueError("No id columns is available in IdaDataFrame:" + column_id +
                             ". Either create a new ID column using add_column_id function" +
                             " or give the name of a column that can be used as ID")

        if self._idadb is None:
            raise IdaNaiveBayesError("The Naive Bayes model was not trained before.")

        # Check or create an outtable name, drop it if it already exists.
        if outtable is None:
            outtable = idadf._idadb._get_valid_tablename('PREDICT_NAIVEBAYES_')
        else:
            outtable = ibmdbpy.utils.check_tablename(outtable)
            if idadf._idadb.exists_table(outtable):
                idadf._idadb.drop_table(outtable)

        if outtableProb is not None:
            outtableProb = ibmdbpy.utils.check_tablename(outtableProb)
            if idadf._idadb.exists_table(outtableProb):
                idadf._idadb.drop_table(outtableProb)

        self.outtable = outtable
        self.outtableProb = outtableProb
        self.mestimation = mestimation

        # Create a temporay view
        idadf.internal_state._create_view()
        tmp_view_name = idadf.internal_state.current_state
        
        if "." in tmp_view_name:
            tmp_view_name = tmp_view_name.split('.')[-1]

        try:
            idadf._idadb._call_stored_procedure("IDAX.PREDICT_NAIVEBAYES ",
                                                 model = self.modelname,
                                                 intable = tmp_view_name,
                                                 id = column_id,
                                                 outtable = self.outtable,
                                                 outtableProb = self.outtableProb,
                                                 mestimation = self.mestimation
                                                 )
        except:
            raise
        finally:
            idadf.internal_state._delete_view()
            idadf._idadb._autocommit()

        self.labels_ = ibmdbpy.IdaDataFrame(idadf._idadb, self.outtable)
        return self.labels_

    def fit_predict(self, idadf, column_id="ID", incolumn=None, coldeftype=None,
                    coldefrole=None, colprepertiesTable=None, outtable = None,
                    outtableProb = None, mestimation = False, verbose=False):
        """
        Convenience function for fitting the model and using it to make 
        predictions about the same dataset. See to fit and predict 
        documentation for an explanation about their attributes.
        """
        self.fit(idadf, column_id, incolumn, coldeftype, coldefrole, colprepertiesTable, verbose)
        return self.predict(idadf, column_id, outtable, outtableProb, mestimation)

    def describe(self):
        """
        Return a description of Naives Bayes.
        """
        if self._idadb is None:
            return self.get_params
        else:
            try:
                self._retrieve_NaiveBayes_Model(self.modelname, verbose=True)
            except:
                raise


    def _retrieve_NaiveBayes_Model(self, modelname, verbose = False):
        """
        Retrieve information about the model to print the results. The Naive 
        Bayes IDAX function stores its result in 2 tables:
            * <MODELNAME>_MODEL
            * <MODELNAME>_DISCRANGES

        Parameters
        ----------
        modelname : str
            The name of the model that is retrieved.

        verbose : bol, default: False
            Verbosity mode.

        Notes
        -----
        Needs better formatting instead of printing the tables.
        """
        modelname = ibmdbpy.utils.check_tablename(modelname)

        if self._idadb is None:
            raise IdaNaiveBayesError("The Naive Bayes model was not trained before.")

        model_main = self._idadb.ida_query('SELECT * FROM "' +
        self._idadb.current_schema + '"."' + modelname + '_MODEL"')
        model_main.columns = ['ATTRIBUTE', 'VAL', 'CLASS', 'CLASSVALCOUNT', 'ATTRCLASSCOUNT',
       'CLASSCOUNT', 'TOTALCOUNT']
        model_main.columns = [x.upper() for x in model_main.columns]

        disc = self._idadb.ida_query('SELECT * FROM "' +
        self._idadb.current_schema + '"."' + modelname + '_DISCRANGES"')
        disc.columns = ['COLNAME', 'BREAK']
        disc.columns = [x.upper() for x in disc.columns]

        if verbose is True:
            print("MODEL")
            print(model_main)
            print("DISCRANGES")
            print(disc)

        return