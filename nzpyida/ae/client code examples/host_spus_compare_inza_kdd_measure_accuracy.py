from nzpyida import IdaDataBase, IdaDataFrame

from nzpyida.ae import NZFunTApply, NZClassTApply
from nzpyida.ae import NZFunGroupedApply

idadb = IdaDataBase('kddcup99', 'admin', 'password')
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


    for name, group in result:
        # print(name)

        # print(group)
        def decision_tree_classifier(df):
            imputed_df = df.copy()



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



            self.output([len(imputed_df), name, np.mean(cvscores_3)])

        ml_result = decision_tree_classifier(df=group)




output_signature = {'dataset_size':'int', 'protocol_type':'str', 'classifier_accuracy':'float'}

import time
start = time.time()
nz_tapply = NZFunTApply(df=idadf, fun_ref =decision_tree_ml_host,  parallel=False,  output_signature=output_signature)
result = nz_tapply.get_result()
result = result.as_dataframe()
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
nz_groupapply = NZFunGroupedApply(df=idadf, index='PROTOCOL_TYPE', fun_ref=decision_tree_ml,  output_signature=output_signature)
result = nz_groupapply.get_result()
result = result.as_dataframe()
print("Parallel Execution on SPUs: \n")
print(result)
end = time.time()
print(end - start)