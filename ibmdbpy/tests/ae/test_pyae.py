import pytest
from ibmdbpy import IdaDataBase, IdaDataFrame
from ibmdbpy.ae import NZFunTApply


def test_host_spus_compare_inza_weather_train_pred():
    idadb = IdaDataBase('weather', 'admin', 'password')
    print(idadb)

    idadf = IdaDataFrame(idadb, 'WEATHER')
    # print(idadf.describe())
    query = 'select * from weather limit 10000'
    # query= "select * from _V_SYS_COLUMNS where TABLE_NAME='WEATHER';"

    df = idadf.ida_query(query)
    # print(df)
    # print(idadf.dtypes)

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

    # file_to_persist = open("file_persist.py", "wb")
    # pickle.dump(decision_tree_ml, file_to_persist)
    # file_to_persist.close()

    dtypes = idadf.dtypes
    output_columns = df.columns.values.tolist()

    # print(output_columns)
    # output_columns = dtypes['COLNAME']
    # print(output_columns)

    output_columns_types = []
    for column in df.dtypes:
        # print(column)
        output_columns_types.append(column.name)
        # print(idadf)

    # output_signature = dict(zip(output_columns, output_columns_types))
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
    groups = result.groupby("LOCATION")
    for name, group in groups:
        print(name + ":" + str(len(group)))


