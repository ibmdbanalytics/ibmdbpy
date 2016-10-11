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
In-Database Association Rules Mining
"""
from __future__ import print_function
from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import
from builtins import dict
from future import standard_library
standard_library.install_aliases()

import ibmdbpy
from ibmdbpy.exceptions import IdaAssociationRulesError
import six

#----------------------------------------------------------------------
# AssociationRules class
class AssociationRules(object):
    """
    Association rules mining can be used to discover interesting and useful 
    relations between items in a large-scale transaction table. You can 
    identify strong rules between related items by using different measures of 
    relevance. Apriori or FP-Growth are well-known algorithms for association 
    rules mining. For analytic stored procedures, the PrefixSpan algorithm is 
    preferred due to its scalability.

    The AssociationRules class provides an interface for using the
    ASSOCRULES amd PREDICT_ASSOCRULES IDAX methods of dashDB/DB2.
    """

    def __init__(self, modelname = None, minsupport = None, maxlen = 5, maxheadlen = 1, minconf = 0.5):
        """
        Constructor for association rules model

        Parameters
        ----------
        modelname : str
            The name of the Association Rules model that is built. If the 
            parameter corresponds to an existing model in the database, it 
            will be replaced during the fitting step.
        minsupport : float or integer, optional
            The minimum fraction (0.0 - 1.0) or the minimum number (above 1) of
            transactions that must contain a pattern to be considered as frequent.
            Default: system-determined
            Range: >0.0 and <1.0 for a minimum fraction
                   >1 for a minimum number of transactions.
        maxlen : int, optional, >=2, default: 5
            The maximum length of a pattern or a rule, that is,
            the maximum number of items per pattern or rule.
        maxheadlen : int, optional, >= 1 and <maxlen, default: 1
            The maximum length of a rule head, that is, the maximum number of
            items that might belong to the item set on the right side of a rule.
            Increasing this value might significantly increase the number of detected rules.
        minconf : float, optional, >=0.0 and <= 1, default: 0.5
            The minimum confidence that a rule must achieve to be kept in the model of the pattern.
            
        Attributes
        ----------
        TODO

        Returns
        -------
        The AssociationRules object, ready to be used for fitting and prediction

       	Examples
       	--------
       	>>> idadb = IdaDataBase("BLUDB-TEST")
       	>>> idadf = IdaDataFrame(idadb, "GROCERIES")
       	>>> arules = AssociationRules("ASSOCRULES_TEST")
       	>>> arules.fit(idadf, transaction_id = "TID", item_id = "SID")

        Notes
        -----
        Inner parameters of the model can be printed and modified by using 
        get_params and set_params. But we recommend creating a new 
        AssociationRules model instead of modifying it.

        """
        # Get set at fit step
        self._idadf = None
        self._idadb = None
        self._transaction_id = None
        self._item_id = None
        self.nametable = None
        self.namecol = None

        # Get set at predict step
        self.outtable = None
        self.type = None
        self.limit = None
        self.sort = None

        self.modelname = modelname
        self.minsupport = minsupport
        self.maxlen = maxlen
        self.maxheadlen = maxheadlen
        self.minconf = minconf



    def get_params(self):
        """
        Return the parameters of the Association Rules model.
        """
        params = dict()
        params['modelname'] = self.modelname
        params['minsupport'] = self.minsupport
        params['maxlen'] = self.maxlen
        params['maxheadlen'] = self.maxheadlen
        params['minconf'] = self.minconf

        params['nametable'] = self.nametable
        params['namecol'] = self.namecol

        params['outtable'] = self.outtable
        params['type'] = self.type
        params['limit'] = self.limit
        params['sort'] = self.sort
        return params

    def set_params(self, **params):
        """
        Modify the parameters of the Association Rules model.
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

    def fit(self, idadf, transaction_id, item_id,  nametable=None, namecol=None, verbose=False):
        """
        Create an Association Rules model from an IdaDataFrame.

        Parameters
        ----------
        idadf : IdaDataFrame
            The IdaDataFrame to be used as input.
        transaction_id : str
            The column of the input table that identifies the transaction ID.
        item_id : str
            The column of the input table that identifies an item of the transaction.
        nametable : str, optional
            The table that contains a mapping of the items in the input table and their names.
            The table must contain at least two columns, where
            * The first column has the same name as the column that is
            contained in the item parameter of the input table
            * The second column has the same name as the name that is
            defined in the namecol parameter
        namecol : str, optional
            The column that contains the item name that is defined in the
            nametable parameter. You cannot specify this parameter if the
            nametable parameter is not specified.
        """
        if not type(idadf).__name__ == 'IdaDataFrame':
            raise TypeError("Argument should be an IdaDataFrame")
        if transaction_id not in idadf.columns:
            raise ValueError("transaction_id is not a column in " + idadf.name)
        if item_id is None:
            raise ValueError("item_id cannot be None (should be a column in " + idadf.name + ")")
        if item_id not in idadf.columns:
            raise ValueError("item_id is not a column in " + idadf.name)

        idadf._idadb._check_procedure("ASSOCRULES", "Association Rules")

        # Check the ID
        if transaction_id not in idadf.columns:
            raise ValueError("Transaction id column"+ transaction_id +" is not available in IdaDataFrame:" )

        self._idadb = idadf._idadb
        self._idadf = idadf
        self._transaction_id = transaction_id
        self._item_id = item_id
        self.nametable = nametable
        self.namecol = namecol

        # Check or create a model name
        if self.modelname is None:
            self.modelname = idadf._idadb._get_valid_modelname('ASSOCRULES_')
        else:
            self.modelname = ibmdbpy.utils.check_tablename(self.modelname)
            if idadf._idadb.exists_model(self.modelname):
                idadf._idadb.drop_model(self.modelname)

        # Create a temporay view
        idadf.internal_state._create_view()
        tmp_view_name = idadf.internal_state.current_state
        
        if "." in tmp_view_name:
            tmp_view_name = tmp_view_name.split('.')[-1]

        try:
            idadf._idadb._call_stored_procedure("IDAX.ASSOCRULES ",
                                                 model = self.modelname,
                                                 intable = tmp_view_name,
                                                 tid = transaction_id,
                                                 item = item_id,
                                                 minsupport = self.minsupport,
                                                 maxlen = self.maxlen,
                                                 maxheadlen = self.maxheadlen,
                                                 minconf = self.minconf,
                                                 nametable = self.nametable,
                                                 namecol = self.namecol)
        except:
            raise
        finally:
            idadf.internal_state._delete_view()
            idadf.commit()

        self._retrieve_AssociationRules_Model(self.modelname, verbose)
        return

    def prune(self, itemsin = None, itemsout = None, minlen = 1, maxlen = None,
              minsupport = 0, maxsupport = 1, minlift = None, maxlift = None,
              minconf = None, maxconf = None, reset = False):
        """
        Prune the rules and patterns of an association rules model. To remove 
        rules and pattern which you are not interested in, you can use filters 
        to exclude these rules and patterns. These rules and patterns are then 
        marked as not valid in the model and are no longer shown.


        Parameters
        ----------
        itemsin : str or list, optional
            A list of item names that must be contained in the rules or
            patterns to be kept. The items are separated by semicolons. At
            least one of the listed items must be contained in a rule or
            pattern to be kept.
            For rules, the following conditions apply:
                * To indicate that the item must be contained in the head of
                then rule, the item names can be succeeded by :h or :head.
                * To indicate that the item must be contained in the body of
                the rule, the item names can be succeeded by :b or :body
            If this parameter is not specified, no constraint is applied.

        itemsout : str or list, optional
            A list of item names that must not be contained in the rules or
            patterns to be kept. The items are separated by semicolons.
            If this parameter is not specified, no constraint is applied.

        minlen : int, optional, >=1, default: 1
            The minimum number of items that are to be kept in the rules or
            patterns.

        maxlen : int, optional, >=1, default: the longest pattern of the model
            The maximum number of items that are to be kept in the rules or
            patterns.

        minsupport : float, optional, >=0.0 and <=maxsupport, default : 0
            The minimum support for the rules or patterns that are to be kept.

        maxsupport : float, optional, >=minsupport and <=1.0, default : 1
            The maximum support for the rules or patterns that are to be kept.

        minlift : float, optional, >=0.0 and <=maxlift, defaukt : 0
            The minimum lift of the rules or patterns that are to be kept.

        maxlift : float, optional, >=minlift, default: the maximum lift of the patterns of the model
            The maximum lift of the rules or patterns that are to be kept.

        minconf : float, optional, >=0.0 and <= maxconf, default : 0
            The minimum confidence of the rules that are to be kept.

        maxconf : float, optional, >=minconf and <= 1.0, default : 1
            The maximum confidence of the rules that are to be kept.

        reset : bool, optional, default: false
            If you specify reset=true, all rules and patterns are first reset
            to not pruned.
            If you specify reset=true or reset=false, the rules and patterns
            that are not to be kept are marked as pruned.
        """

        self._idadf._idadb._check_procedure("PRUNE_ASSOCRULES", "Pruning for Association Rules")

        if isinstance(itemsin, list):
            itemsin = ";".join(itemsin)

        try:
            self._idadf._idadb._call_stored_procedure("IDAX.PRUNE_ASSOCRULES ",
                                                 model = self.modelname,
                                                 itemsin = itemsin,
                                                 itemsout = itemsout,
                                                 minlen = minlen,
                                                 maxlen = maxlen,
                                                 minsupport = minsupport,
                                                 maxsupport = maxsupport,
                                                 minlift = minlift,
                                                 maxlift = maxlift,
                                                 minconf = minconf,
                                                 maxconf = maxconf,
                                                 reset = reset)
        except:
            raise
        else:
        	return

    def predict(self, idadf, outtable=None, transaction_id=None, item_id=None,
                type="rules", limit=1, sort=None):
        """
        Apply the rules and patterns of an association rules model to other
        transactions. You can apply all rules or only specific rules according
        to specified criteria.

        Parameters
        ----------
        idadf : IdaDataFrame
            IdaDataFrame to be used as input.

        outtable : str, optional
            The name of the output table in which the mapping between the input 
            sequences and the associated rules or patterns is written. If the 
            parameter corresponds to an existing table in the database, it is 
            replaced.

        transaction_id : str, optional
            The column of the input table that identifies the transaction ID. 
            By default, this is the same tid column that is specified in the 
            stored procedure to build the model.


        item_id : str, optional
            The column of the input table that identifies an item of the 
            transaction. By default, this is the same item column that is 
            specified in the stored procedure to build the model.


        type : str, optional, default : "rules"
            The type of information that is written in the output table. The 
            following values are possible: ‘rules’ and ‘patterns’.

        limit : int, optional, >=1, default: 1
            The maximum number of rules or patterns that is written in the 
            output table for each input sequence.

        sort : str or list, optional
            A list of keywords that indicates the order in which the rules or 
            patterns are written in the output table. The order of the list is 
            descending. The items are separated by semicolons. The following 
            values are possible: ‘support’, ‘confidence’, ‘lift’, and ‘length’. 
            The ‘confidence’ value can only be specified if the type parameter 
            is ‘rules’. If the type parameter is ‘rules’, the default is: 
            support;confidence;length.  If the type parameter is ‘patterns’, 
            the default is: support;lift;length. 

        Notes
        -----
        When "type" is set to "rules", it looks like nothing is returned.
        """
        if not isinstance(idadf, ibmdbpy.IdaDataFrame):
            raise TypeError("Argument should be an IdaDataFrame")

        if sort is not None:
            sort = ';'.join(sort)

        if transaction_id is None:
            transaction_id = self.transaction_id
        if item_id is None:
            item_id = self.item_id

        # Check the ID
        if transaction_id not in idadf.columns:
            raise ValueError("Transaction id column"+ transaction_id +" is not available in IdaDataFrame." )

        if self._idadb is None:
            raise IdaAssociationRulesError("No Association rules model was trained before.")

        # The version where we don't replace the outtable if it exists but raise an exception
        #if outtable is not None:
        #    if idadf._idadb.exists_table(outtable):
        #        raise ValueError("Table "+ outtable +" already exists.")
        #else:
        #    outtable = idadf._idadb._get_valid_modelname('PREDICT_ASSOCRULES_')

        if self.outtable is None:
            self.outtable = idadf._idadb._get_valid_tablename('ASSOCRULES_')
        else:
            self.outtable = ibmdbpy.utils.check_tablename(self.outtable)
            if idadf._idadb.exists_table(self.outtable):
                idadf._idadb.drop_table(self.outtable)

        self.outtable = outtable
        self.type = type
        self.limit = limit
        self.sort = sort

        # Create a temporay view
        idadf.internal_state._create_view()
        tmp_view_name = idadf.internal_state.current_state
        
        if "." in tmp_view_name:
            tmp_view_name = tmp_view_name.split('.')[-1]

        try:
            idadf._idadb._call_stored_procedure("IDAX.PREDICT_ASSOCRULES ",
                                                 model = self.modelname,
                                                 intable = tmp_view_name,
                                                 outtable = outtable,
                                                 tid = transaction_id,
                                                 item = item_id,
                                                 type = type,
                                                 limit = limit,
                                                 sort = sort
                                                 )
        except:
            raise
        finally:
            idadf.internal_state._delete_view()
            idadf._cursor.commit()

        self.labels_ = ibmdbpy.IdaDataFrame(idadf._idadb, outtable)
        return self.labels_

    def fit_predict(self, idadf, transaction_id, item_id,  nametable=None,
    				namecol=None, outtable=None, type="rules", limit=1,
                    sort=None, verbose=False):
        """
        Convenience function for fitting the model and using it to make 
        predictions about the same dataset. See the fit and predict 
        documentation for an explanation about their attributes.


        Notes
        -----
        If you use this function, you are not able to use the prune step 
        between the fit and the predict step. However, you can still prune 
        afterwards and reuse the predict function.
        """
        #TODO: Is it relevant ? Result of predict on the same dataset looks empty
        #for type "rules"
        self.fit(idadf, transaction_id, item_id,  nametable, namecol, verbose)
        return self.predict(idadf, outtable, transaction_id, item_id, type, limit, sort)

    def describe(self):
        """
        Return a description of Association Rules Model.
        """
        if self._idadb is None:
            return self.get_params
        else:
            try:
                # Not sure it is useful
                res = self._idadb._call_stored_procedure("IDAX.PRINT_MODEL ", model = self.modelname)
                self._retrieve_AssociationRules_Model(self.modelname, verbose=True)
            except:
                raise
            else:
                print(res)
            return

    def _retrieve_AssociationRules_Model(self, modelname, verbose = False):
        """
        Retrieve information about the model to print the results. The 
        Association Rules IDAX function stores its result in 4 tables:
            * <MODELNAME>_ASSOCPATTERNS
            * <MODELNAME>_ASSOCPATTERNS_STATISTICS
            * <MODELNAME>_ASSOCRULES
            * <MODELNAME>_ITEMS

        Parameters
        ----------
        modelname : str
            The name of the model that is retrieved.
        verbose : bol, default: False
            Verbosity mode.

        Notes
        -----
        Needs better formatting instead of printing the tables
        """
        modelname = ibmdbpy.utils.check_tablename(modelname)

        if self._idadb is None:
            raise IdaAssociationRulesError("No Association rules model was trained before.")

        # Note: The name of the columns in hardcoded, this is done so as a 
        # workaround for some bug in a specific ODBC linux driver. 
        # In case the implementation of the IDA method changes, this may break
        # But still would not be difficult to fix 

        assocpatterns = self._idadb.ida_query('SELECT * FROM "' +
        self._idadb.current_schema + '"."' + modelname + '_ASSOCPATTERNS"')
        assocpatterns.columns = ["ITEMSETID","ITEMID"]
        assocpatterns.columns = [x.upper() for x in assocpatterns.columns]

        assocpatterns_stats = self._idadb.ida_query('SELECT * FROM "' +
        self._idadb.current_schema + '"."' + modelname + '_ASSOCPATTERNS_STATISTICS"')
        assocpatterns_stats = ["ITEMSETID" , "LENGTH" , "COUNT"  , "SUPPORT" , "LIFT"  ,"PRUNED"]
        assocpatterns_stats.columns = [x.upper() for x in assocpatterns_stats.columns]

        assocrules = self._idadb.ida_query('SELECT * FROM "' +
        self._idadb.current_schema + '"."' + modelname + '_ASSOCRULES"')
        assocrules.columns = ["RULEID", "ITEMSETID", "BODYID", "HEADID", "CONFIDENCE", "PRUNED"]
        assocrules.columns = [x.upper() for x in assocrules.columns]

        items = self._idadb.ida_query('SELECT * FROM "' +
        self._idadb.current_schema + '"."' + modelname + '_ITEMS"')
        items.columns = ["ITEMID","ITEM","ITEMNAME","COUNT","SUPPORT"]
        items.columns = [x.upper() for x in items.columns]

        if verbose is True:
            print("assocpatterns")
            print(assocpatterns)

            print("assocpatterns_stats")
            print(assocpatterns_stats)

            print("assocrules")
            print(assocrules)

            print("items")
            print(items)

        return