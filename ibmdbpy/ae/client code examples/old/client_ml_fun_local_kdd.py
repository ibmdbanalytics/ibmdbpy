


from ibmdbpy import IdaDataBase, IdaDataFrame

import time
start = time.time()
idadb = IdaDataBase('weather', 'admin', 'password')
print(idadb)

idadf = IdaDataFrame(idadb, 'KDDCUP99')
query = 'select * from kddcup99 '

df = idadf.ida_query(query)
print(df.dtypes)
print(df.head(5))

db_name = idadf.internal_state.current_state




def decision_tree_ml(df):
    import numpy as np

    from sklearn.model_selection import cross_val_score
    from sklearn.impute import SimpleImputer
    from sklearn.tree import DecisionTreeClassifier

    from sklearn.preprocessing import LabelEncoder
    # data preparation
    imputed_df = df.copy()
    ds_size = len(imputed_df)
    #print(imputed_df['attack_type'])
    #location = imputed_df.location[0]


    #imputed_df['cloud9am'] = imputed_df.cloud9am.astype('str')
    #imputed_df['cloud3pm'] = imputed_df.cloud3pm.astype('str')
    #imputed_df['sunshine'] = imputed_df.sunshine.astype('float')
    #imputed_df['evaporation'] = imputed_df.evaporation.astype('float')


    #imputed_df = imputed_df.drop('date', 1)
    columns = imputed_df.columns



    for column in columns:

        if imputed_df[column].isnull().sum()==len(imputed_df):
            imputed_df=imputed_df.drop(column, axis=1)

    #print(imputed_df.head(5))
    columns = imputed_df.columns
    for column in columns:

        if (imputed_df[column].dtype == 'float64' or imputed_df[column].dtype == 'int64'):
            #print("numerical column is" + column)
            #print(imputed_df[column].unique())
            #print(imputed_df[column])
            imp = SimpleImputer(missing_values=np.nan, strategy='mean')

            transformed_column = imp.fit_transform(imputed_df[column].values.reshape(-1, 1))
            #print(transformed_column)

            # transformed_column = transformed_column.flatten()
            #print(transformed_column.shape)
            imputed_df[column] = transformed_column
            #print(imputed_df[column])

        if (imputed_df[column].dtype == 'object'):
            # impute missing values for categorical variables
            #print("categorical column is" + column)
            #print(imputed_df[column])

            imp = SimpleImputer(missing_values=None, strategy='constant', fill_value='missing')
            imputed_df[column] = imp.fit_transform(imputed_df[column].values.reshape(-1, 1))
            imputed_df[column] = imputed_df[column].astype('str')
            #print(imputed_df[column])
            le = LabelEncoder()
            #print(imputed_df[column].unique())

            le.fit(imputed_df[column].unique())
            #print(le.classes_)
            imputed_df[column] = le.transform(imputed_df[column])

    X = imputed_df.drop('attack_type', axis=1)
    y = imputed_df['attack_type']

    # Create a decision tree
    dt = DecisionTreeClassifier(max_depth=5)

    cvscores_3 = cross_val_score(dt, X, y, cv=3)

    return ds_size, np.mean(cvscores_3)



def grouped_local(index,db_name,df):


    #df = data_preparation(df)
    #print(df.head(10))
    result = df.groupby('protocol_type')

    #result = idadf.ida_query(query, autocommit=True)

    for name, group in result:
        print(name)

        #print(group)
        ml_result = decision_tree_ml(df=group)
        print(ml_result)



grouped_local(index='location', db_name='ADMIN.weather',df=df)
end = time.time()
print(end-start)



def apply_fun(self, x):
    from math import sqrt
    max_temp = x[3]
    fahren_max_temp = (max_temp*1.8)+32
    return fahren_max_temp, max_temp


