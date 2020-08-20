

from ibmdbpy import IdaDataBase, IdaDataFrame

from ibmdbpy.ae import NZFunTApply, NZClassTApply
from ibmdbpy.ae import NZFunApply
from ibmdbpy.ae import NZFunGroupedApply

idadb = IdaDataBase('weather', 'admin', 'password')
print(idadb)

idadf = IdaDataFrame(idadb, 'KDDCUP99')
print(idadf.head())



def decision_tree_ml_host(self, df):

    from sklearn.model_selection import cross_val_score
    from sklearn.impute import SimpleImputer
    from sklearn.tree import DecisionTreeClassifier

    from sklearn.preprocessing import LabelEncoder
    import numpy as np


    result = df.groupby('PROTOCOL_TYPE')

    # result = idadf.ida_query(query, autocommit=True)

    for name, group in result:
        # print(name)

        # print(group)
        def decision_tree_classifier(df):
            imputed_df = df.copy()

            #imputed_df['CLOUD9AM'] = imputed_df.CLOUD9AM.astype('str')
            #imputed_df['CLOUD3PM'] = imputed_df.CLOUD3PM.astype('str')
            #imputed_df['SUNSHINE'] = imputed_df.SUNSHINE.astype('float')
            #imputed_df['EVAPORATION'] = imputed_df.EVAPORATION.astype('float')

            # remove columns which have only null values
            columns = imputed_df.columns
            for column in columns:
                if imputed_df[column].isnull().sum() == len(imputed_df):
                    imputed_df = imputed_df.drop(column, 1)

            columns = imputed_df.columns

            for column in columns:

                if (imputed_df[column].dtype == 'float64' or imputed_df[column].dtype == 'int64'):
                    imp = SimpleImputer(missing_values=np.nan, strategy='mean')
                    imputed_df[column] = imp.fit_transform(imputed_df[column].values.reshape(-1, 1))

                if (imputed_df[column].dtype == 'object'):
                    # impute missing values for categorical variables
                    imp = SimpleImputer(missing_values=None, strategy='constant', fill_value='missing')
                    imputed_df[column] = imp.fit_transform(imputed_df[column].values.reshape(-1, 1))
                    imputed_df[column] = imputed_df[column].astype('str')
                    le = LabelEncoder()
                    # print(imputed_df[column].unique())

                    le.fit(imputed_df[column].unique())
                    # print(le.classes_)
                    imputed_df[column] = le.transform(imputed_df[column])

            X = imputed_df.drop('ATTACK_TYPE', axis=1)
            y = imputed_df['ATTACK_TYPE']

            # Create a decision tree
            dt = DecisionTreeClassifier(max_depth=5)

            cvscores_3 = cross_val_score(dt, X, y, cv=3)

            return len(imputed_df), name, np.mean(cvscores_3)

        ml_result = decision_tree_classifier(df=group)
        self.output(ml_result)









#nz_tapply = NZFunTApply(df=idadf, fun=decision_tree_ml, parallel=True,output_signature=["data_size=int", "location=str", "classifier_score=double"])
#result = nz_tapply.get_result()
#print("\n")
#print(result)

def apply_fun(self, x):
    from math import sqrt
    max_temp = x[3]
    fahren_max_temp = (max_temp*1.8)+32
    return fahren_max_temp, max_temp

#nz_apply = NZFunApply(df=idadf, fun=apply_fun, output_signature=["fahren_max_temp=double", "max_temp=double"], output_table='weather_new' )
#result = nz_apply.get_result()
#print(result)

import time
start = time.time()
nz_groupapply = NZFunTApply(df=idadf, fun=decision_tree_ml_host,  parallel=False, output_signature=["dataset_size=int", "location=str", "classifier_accuracy=double"])
result = nz_groupapply.get_result()
print("Host only execution")
print(result)
print("\n")

end = time.time()
print(end - start)

def decision_tree_ml(self, df):
    from sklearn.model_selection import cross_val_score
    from sklearn.impute import SimpleImputer
    from sklearn.tree import DecisionTreeClassifier

    from sklearn.preprocessing import LabelEncoder
    import numpy as np

    location = df.PROTOCOL_TYPE[0]

    # data preparation
    imputed_df = df.copy()
    ds_size = len(imputed_df)
    #imputed_df['CLOUD9AM'] = imputed_df.CLOUD9AM.astype('str')
    #imputed_df['CLOUD3PM'] = imputed_df.CLOUD3PM.astype('str')
    #imputed_df['SUNSHINE'] = imputed_df.SUNSHINE.astype('float')
    #imputed_df['EVAPORATION'] = imputed_df.EVAPORATION.astype('float')


    #remove columns which have only null values
    columns = imputed_df.columns
    for column in columns:
        if imputed_df[column].isnull().sum()==len(imputed_df):
            imputed_df=imputed_df.drop(column, 1)

    columns = imputed_df.columns

    for column in columns:

        if (imputed_df[column].dtype == 'float64' or imputed_df[column].dtype == 'int64'):
            imp = SimpleImputer(missing_values=np.nan, strategy='mean')
            imputed_df[column] = imp.fit_transform(imputed_df[column].values.reshape(-1, 1))

        if (imputed_df[column].dtype == 'object'):
            # impute missing values for categorical variables
            imp = SimpleImputer(missing_values=None, strategy='constant', fill_value='missing')
            imputed_df[column] = imp.fit_transform(imputed_df[column].values.reshape(-1, 1))
            imputed_df[column] = imputed_df[column].astype('str')
            le = LabelEncoder()
            #print(imputed_df[column].unique())

            le.fit(imputed_df[column].unique())
            # print(le.classes_)
            imputed_df[column] = le.transform(imputed_df[column])



    X = imputed_df.drop('ATTACK_TYPE', axis=1)
    y = imputed_df['ATTACK_TYPE']

    # Create a decision tree
    dt = DecisionTreeClassifier(max_depth=5)

    cvscores_3 = cross_val_score(dt, X, y, cv=3)

    self.output(ds_size, location, np.mean(cvscores_3))




import time
start = time.time()
nz_groupapply = NZFunGroupedApply(df=idadf, index='PROTOCOL_TYPE', fun=decision_tree_ml,  output_signature=["dataset_size=int", "location=str", "classifier_accuracy=double"])
result = nz_groupapply.get_result()
print("Parallel Execution on SPUs: \n")
print(result)
end = time.time()
print(end - start)

