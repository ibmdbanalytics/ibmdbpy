import pytest
from ibmdbpy import IdaDataBase, IdaDataFrame
from ibmdbpy.ae import NZFunTApply, NZFunApply, NZFunGroupedApply
from ibmdbpy.ae import NZInstall


def test_tapply_host_weather_train_pred():
    idadb = IdaDataBase('weather', 'admin', 'password')
    print(idadb)

    idadf = IdaDataFrame(idadb, 'WEATHER')


    code_str_host = """def decision_tree_ml_host(self, df):

        from sklearn.model_selection import cross_val_score
        from sklearn.impute import SimpleImputer
        from sklearn.tree import DecisionTreeClassifier
        from sklearn.model_selection import train_test_split

        from sklearn.preprocessing import LabelEncoder
        import numpy as np

        result = df.groupby('LOCATION')
        #result = df.groupby(pd.qcut(df['ID'], q=3))

        # result = idadf.ida_query(query, autocommit=True)

        for name, group in result:
            # print(name)

            # print(group)
            def decision_tree_classifier(df):
                imputed_df = df.copy()
                ds_size = len(imputed_df)
                temp_dict = dict()





                columns = imputed_df.columns

                for column in columns:
                    if column=='ID':
                        continue

                    if (imputed_df[column].dtype == 'float64' or imputed_df[column].dtype == 'int64'):
                        if imputed_df[column].isnull().sum()==len(imputed_df):
                         imputed_df[column] = imputed_df[column].fillna(0)

                        else :
                         imp = SimpleImputer(missing_values=np.nan, strategy='mean')
                         transformed_column = imp.fit_transform(imputed_df[column].values.reshape(-1, 1))
                         imputed_df[column] = transformed_column


                    if (imputed_df[column].dtype == 'object'):
                        # impute missing values for categorical variables
                        imp = SimpleImputer(missing_values=None, strategy='constant', fill_value='missing')
                        imputed_df[column] = imp.fit_transform(imputed_df[column].values.reshape(-1, 1))
                        imputed_df[column] = imputed_df[column].astype('str')
                        le = LabelEncoder()
                        # print(imputed_df[column].unique())

                        le.fit(imputed_df[column])
                        # print(le.classes_)
                        imputed_df[column] = le.transform(imputed_df[column])
                        temp_dict[column] = le

                X = imputed_df.drop(['RISK_MM', 'RAINTOMORROW'], axis=1)
                y = imputed_df['RAINTOMORROW']
                X_train, X_test, y_train, y_test = train_test_split(X,y, test_size = 0.25, random_state=42, stratify=y)
                #X_train_mod = X_train.drop(['RISK_MM'],axis=1)
                #X_test_mod = X_test.drop(['RISK_MM'],axis=1)
                dt = DecisionTreeClassifier(max_depth=5)
                dt.fit(X_train, y_train)

                accuracy = dt.score(X_test, y_test)    
                #print(accuracy)



                pred_df = X_test.copy()


                y_pred= dt.predict(X_test)

                pred_df['RAINTOMORROW'] = y_pred
                pred_df['DATASET_SIZE'] = ds_size
                pred_df['CLASSIFIER_ACCURACY']=round(accuracy,2)




                original_columns = pred_df.columns

                for column in original_columns:

                 if column in temp_dict:   
                   pred_df[column] = temp_dict[column].inverse_transform(pred_df[column])
                   #print(pred_df)

                def print_output(x):
                    row = [x['ID'], x['RAINTOMORROW'], x['DATASET_SIZE'], x['CLASSIFIER_ACCURACY']]
                    self.output(row)


                pred_df.apply(print_output, axis=1)

                return pred_df


            ml_result = decision_tree_classifier(df=group)

            """



    output_signature = {'ID': 'int', 'RAINTOMORROW_PRED': 'str', 'DATASET_SIZE': 'int', 'CLASSIFIER_ACCURACY': 'float'}

    import time
    start = time.time()

    nz_tapply = NZFunTApply(df=idadf, code_str=code_str_host, fun_name='decision_tree_ml_host', parallel=False,
                            output_signature=output_signature, merge_output_with_df=True)
    result = nz_tapply.get_result()
    print("\n")
    print(result)
    end = time.time()
    print(end - start)
    print(result.shape[0])
    assert result.shape[0] == 35565, "number of records are not matching"
    assert result.shape[1] == 28, "number of columns are not matching"


def test_apply_weather_save_table_merge_withdf():
    idadb = IdaDataBase('weather', 'admin', 'password')
    print(idadb)

    idadf = IdaDataFrame(idadb, 'WEATHER')
    code_str_apply = """def apply_fun(self, x):
        from math import sqrt
        max_temp = x[3]
        id = x[24]
        fahren_max_temp = (max_temp*1.8)+32
        row = [id, max_temp,  fahren_max_temp]
        self.output(row)
        """
    output_signature = {'ID': 'int', 'MAX_TEMP': 'float', 'FAHREN_MAX_TEMP': 'float'}
    idadb.drop_table("temp_conversion")
    nz_apply = NZFunApply(df=idadf, code_str=code_str_apply, fun_name='apply_fun', output_table="temp_conversion",
                          output_signature=output_signature, merge_output_with_df=True)
    result = nz_apply.get_result()
    print(result)
    assert result.shape[0] == 142193, "number of records are not matching"
    assert result.shape[1] == 27, "number of columns are not matching"

def test_apply_weather_merge_withdf():
        idadb = IdaDataBase('weather', 'admin', 'password')


        idadf = IdaDataFrame(idadb, 'WEATHER')
        code_str_apply = """def apply_fun(self, x):
            from math import sqrt
            max_temp = x[3]
            id = x[24]
            fahren_max_temp = (max_temp*1.8)+32
            row = [id, max_temp,  fahren_max_temp]
            self.output(row)
            """
        output_signature = {'ID': 'int', 'MAX_TEMP': 'float', 'FAHREN_MAX_TEMP': 'float'}
        nz_apply = NZFunApply(df=idadf, code_str=code_str_apply, fun_name='apply_fun',
                              output_signature=output_signature, merge_output_with_df=True)
        result = nz_apply.get_result()
        print(result)
        assert result.shape[0] == 142193, "number of records are not matching"
        assert result.shape[1] == 27, "number of columns are not matching"

def test_apply_weather():
            idadb = IdaDataBase('weather', 'admin', 'password')
            print(idadb)

            idadf = IdaDataFrame(idadb, 'WEATHER')
            code_str_apply = """def apply_fun(self, x):
                from math import sqrt
                max_temp = x[3]
                id = x[24]
                fahren_max_temp = (max_temp*1.8)+32
                row = [id, max_temp,  fahren_max_temp]
                self.output(row)
                """
            output_signature = {'ID': 'int', 'MAX_TEMP': 'float', 'FAHREN_MAX_TEMP': 'float'}
            nz_apply = NZFunApply(df=idadf, code_str=code_str_apply, fun_name='apply_fun',
                                  output_signature=output_signature)
            result = nz_apply.get_result()
            print(result)
            assert result.shape[0] == 142193, "number of records are not matching"
            assert result.shape[1] == 3, "number of columns are not matching"

def test_tapply_weather_host_spus_train_pred():
 idadb = IdaDataBase('weather', 'admin', 'password')
 print(idadb)

 idadf = IdaDataFrame(idadb, 'WEATHER')

 code_str_host_spus = """def decision_tree_ml(self, df):
             from sklearn.model_selection import cross_val_score
             from sklearn.impute import SimpleImputer
             from sklearn.tree import DecisionTreeClassifier
             from sklearn.model_selection import train_test_split

             from sklearn.preprocessing import LabelEncoder
             import numpy as np



             # data preparation
             imputed_df = df.copy()
             ds_size = len(imputed_df)
             temp_dict = dict()


             columns = imputed_df.columns

             for column in columns:
                 if column=='ID':
                     continue

                 if (imputed_df[column].dtype == 'float64' or imputed_df[column].dtype == 'int64'):
                   if imputed_df[column].isnull().sum()==len(imputed_df):
                      imputed_df[column] = imputed_df[column].fillna(0)

                   else :

                      imp = SimpleImputer(missing_values=np.nan, strategy='mean')
                      transformed_column = imp.fit_transform(imputed_df[column].values.reshape(-1, 1))         
                      imputed_df[column] = transformed_column

                 if (imputed_df[column].dtype == 'object'):
                     # impute missing values for categorical variables
                     imp = SimpleImputer(missing_values=None, strategy='constant', fill_value='missing')
                     imputed_df[column] = imp.fit_transform(imputed_df[column].values.reshape(-1, 1))
                     imputed_df[column] = imputed_df[column].astype('str')
                     le = LabelEncoder()

                     le.fit(imputed_df[column])
                     # print(le.classes_)
                     imputed_df[column] = le.transform(imputed_df[column])
                     temp_dict[column] = le



             # Create a decision tree
             dt = DecisionTreeClassifier(max_depth=5)
             X = imputed_df.drop(['RISK_MM', 'RAINTOMORROW'], axis=1)
             y = imputed_df['RAINTOMORROW']
             X_train, X_test, y_train, y_test = train_test_split(X,y, test_size = 0.25, random_state=42, stratify=y)


             dt.fit(X_train, y_train)

             accuracy = dt.score(X_test, y_test)    
             #print(accuracy)



             pred_df = X_test.copy()


             y_pred= dt.predict(X_test)

             pred_df['RAINTOMORROW'] = y_pred
             pred_df['DATASET_SIZE'] = ds_size
             pred_df['CLASSIFIER_ACCURACY']=round(accuracy,2)




             original_columns = pred_df.columns

             for column in original_columns:

              if column in temp_dict:   
                pred_df[column] = temp_dict[column].inverse_transform(pred_df[column])
                #print(pred_df)

             def print_output(x):
                 row = [x['ID'], x['RAINTOMORROW'], x['DATASET_SIZE'], x['CLASSIFIER_ACCURACY']]
                 self.output(row)


             pred_df.apply(print_output, axis=1)






 """

 output_signature = {'ID': 'int', 'RAINTOMORROW_PRED': 'str', 'DATASET_SIZE': 'int', 'CLASSIFIER_ACCURACY': 'float'}
 nz_tapply = NZFunTApply(df=idadf, code_str=code_str_host_spus, fun_name ="decision_tree_ml", parallel=True, output_signature=output_signature)
 result = nz_tapply.get_result()
 print("Host +SPUs execution - slicing on a default column- ML function for the entire slices")
 print(result)
 assert result.shape[0] == 35551, "number of records are not matching"
 assert result.shape[1] == 4, "number of columns are not matching"





def test_groupedapply_weather_host_spus():
    idadb = IdaDataBase('weather', 'admin', 'password')
    print(idadb)

    idadf = IdaDataFrame(idadb, 'WEATHER')

    code_str_host_spus = """def decision_tree_ml(self, df):
                from sklearn.model_selection import cross_val_score
                from sklearn.impute import SimpleImputer
                from sklearn.tree import DecisionTreeClassifier
                from sklearn.model_selection import train_test_split

                from sklearn.preprocessing import LabelEncoder
                import numpy as np



                # data preparation
                imputed_df = df.copy()
                ds_size = len(imputed_df)
                temp_dict = dict()


                columns = imputed_df.columns

                for column in columns:
                    if column=='ID':
                        continue

                    if (imputed_df[column].dtype == 'float64' or imputed_df[column].dtype == 'int64'):
                      if imputed_df[column].isnull().sum()==len(imputed_df):
                         imputed_df[column] = imputed_df[column].fillna(0)

                      else :

                         imp = SimpleImputer(missing_values=np.nan, strategy='mean')
                         transformed_column = imp.fit_transform(imputed_df[column].values.reshape(-1, 1))         
                         imputed_df[column] = transformed_column

                    if (imputed_df[column].dtype == 'object'):
                        # impute missing values for categorical variables
                        imp = SimpleImputer(missing_values=None, strategy='constant', fill_value='missing')
                        imputed_df[column] = imp.fit_transform(imputed_df[column].values.reshape(-1, 1))
                        imputed_df[column] = imputed_df[column].astype('str')
                        le = LabelEncoder()

                        le.fit(imputed_df[column])
                        # print(le.classes_)
                        imputed_df[column] = le.transform(imputed_df[column])
                        temp_dict[column] = le



                # Create a decision tree
                dt = DecisionTreeClassifier(max_depth=5)
                X = imputed_df.drop(['RISK_MM', 'RAINTOMORROW'], axis=1)
                y = imputed_df['RAINTOMORROW']
                X_train, X_test, y_train, y_test = train_test_split(X,y, test_size = 0.25, random_state=42, stratify=y)


                dt.fit(X_train, y_train)

                accuracy = dt.score(X_test, y_test)    
                #print(accuracy)



                pred_df = X_test.copy()


                y_pred= dt.predict(X_test)

                pred_df['RAINTOMORROW'] = y_pred
                pred_df['DATASET_SIZE'] = ds_size
                pred_df['CLASSIFIER_ACCURACY']=round(accuracy,2)




                original_columns = pred_df.columns

                for column in original_columns:

                 if column in temp_dict:   
                   pred_df[column] = temp_dict[column].inverse_transform(pred_df[column])
                   #print(pred_df)

                def print_output(x):
                    row = [x['ID'], x['RAINTOMORROW'], x['DATASET_SIZE'], x['CLASSIFIER_ACCURACY']]
                    self.output(row)


                pred_df.apply(print_output, axis=1)






    """

    output_signature = {'ID': 'int', 'RAINTOMORROW_PRED': 'str', 'DATASET_SIZE': 'int', 'CLASSIFIER_ACCURACY': 'float'}
    import time
    start = time.time()

    nz_groupapply = NZFunGroupedApply(df=idadf, code_str=code_str_host_spus, index='LOCATION',
                                      fun_name="decision_tree_ml", output_signature=output_signature,
                                      merge_output_with_df=True)
    # nz_groupapply = NZFunGroupedApply(df=idadf,  code_str=code_str_host_spus, index='LOCATION', fun_name="decision_tree_ml")
    result = nz_groupapply.get_result()
    print("Host+ SPUs execution - slicing on user selection -ML function for partitions within slices\n")
    print(result)
    print(time.time()-start)
    assert result.shape[0] == 35565, "number of records are not matching"
    assert result.shape[1] == 28, "number of columns are not matching"




def test_install():
    nzinstall = NZInstall(package_name='scikit-learn')
    result = nzinstall.getResultCode()
    assert result==0, "installation failed"
