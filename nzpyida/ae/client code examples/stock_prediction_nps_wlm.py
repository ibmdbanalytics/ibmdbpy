import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from nzpyida import IdaDataBase, IdaDataFrame
from nzpyida.ae import NZFunTApply
from nzpyida.ae import NZFunApply
from nzpyida.ae import NZFunGroupedApply


#connect to database table
dsn = 'bank'

idadb = IdaDataBase(dsn, 'admin', 'password')

idadf = IdaDataFrame(idadb, 'stocks')

print(idadf.head())

#preprocess data - add more features
idadb.drop_table("stocks_indicator_features")
code_str_host_spus = """def all_stocks_add_features(self,df):







        imputed_df = df.copy()
        imputed_df['DATE'] = pd.to_datetime(imputed_df['DATE'])
        imputed_df = imputed_df.sort_values(by='DATE') 
        imputed_df['DATE'] = imputed_df['DATE'].dt.date



        #name = imputed_df.TICKER[0]



        #how many days in future you need to predict
        future_days = -1

        #add the future close price column and shift by the required days

        imputed_df['FUTURE_CLOSE_PRICE'] = imputed_df['ADJCLOSE'].shift(future_days)



        # add technical indicators
        for n in [14,30,50,200]:

          # create the moving average indicator 
          imputed_df['MA'+str(n)] = imputed_df['ADJCLOSE'].rolling(window=n).mean()


        #imputed_df.dropna(inplace=True)

        def print_output(x):
                row = [x['ID'], x['FUTURE_CLOSE_PRICE'], x['MA14'], x['MA30'], x['MA50'], x['MA200']]
                self.output(row)


        imputed_df.apply(print_output, axis=1)








"""

output_signature = {'ID': 'int', 'FUTURE_CLOSE_PRICE': 'float', 'MA14': 'float', 'MA30': 'float', 'MA50': 'float',
                    'MA200': 'float'}

import time

start = time.time()

nz_tapply = NZFunGroupedApply(df=idadf, code_str=code_str_host_spus, fun_name='all_stocks_add_features', index='TICKER',
                              output_table='stocks_indicator_features', output_signature=output_signature,
                              merge_output_with_df=True)
result = nz_tapply.get_result()
print(result.head())

end = time.time()
print(end - start)


#compute train data sequentially

idadf = IdaDataFrame(idadb, 'stocks_indicator_features')
print(idadf.shape)
idadb.drop_table("stocks_features_train")
code_str_host_spus = """def stocks_train_data(self,df):



        imputed_df = df.copy()
        imputed_df['DATE'] = pd.to_datetime(imputed_df['DATE'])
        imputed_df = imputed_df.sort_values(by='DATE') 
        imputed_df['DATE'] = imputed_df['DATE'].dt.date



        #name = imputed_df.TICKER[0]
        train_size = int(0.9*imputed_df.shape[0])



        train_data = imputed_df[0:train_size]



        def print_output(x):
                row = [x['ID']]
                self.output(row)


        train_data.apply(print_output, axis=1)








"""

output_signature = {'ID': 'int'}

import time

start = time.time()

nz_tapply = NZFunGroupedApply(df=idadf, code_str=code_str_host_spus, fun_name='stocks_train_data', index='TICKER',
                              output_table='stocks_features_train', output_signature=output_signature,
                              merge_output_with_df=True)
result = nz_tapply.get_result()
print(result.head())

end = time.time()
print(end - start)

#compute test data sequentially
idadf = IdaDataFrame(idadb, 'stocks_indicator_features')
print(idadf.shape)

if (idadb.exists_table("stocks_features_test")):
    idadb.drop_table("stocks_features_test")
code_str_host_spus = """def stocks_test_data(self,df):







        imputed_df = df.copy()
        imputed_df['DATE'] = pd.to_datetime(imputed_df['DATE'])
        imputed_df = imputed_df.sort_values(by='DATE') 
        imputed_df['DATE'] = imputed_df['DATE'].dt.date



        #name = imputed_df.TICKER[0]
        train_size = int(0.9*imputed_df.shape[0])



        test_data = imputed_df[train_size:]



        def print_output(x):
                row = [x['ID']]
                self.output(row)


        test_data.apply(print_output, axis=1)








"""

output_signature = {'ID': 'int'}

import time

start = time.time()

nz_tapply = NZFunGroupedApply(df=idadf, code_str=code_str_host_spus, fun_name='stocks_test_data', index='TICKER',
                              output_table='stocks_features_test', output_signature=output_signature,
                              merge_output_with_df=True)
result = nz_tapply.get_result()
print(result.head())

end = time.time()
print(end - start)

#build models on nps with train dataset and save to wml

idadf = IdaDataFrame(idadb, 'stocks_features_train')

code_str_host_spus = """def stocks_rf_ml(self, df):

    import numpy as np

    from sklearn.impute import SimpleImputer
    from sklearn.metrics import mean_squared_error
    from sklearn.ensemble import RandomForestRegressor
    from ibm_watson_machine_learning import APIClient
    from sklearn.model_selection import train_test_split

    wml_credentials = {
                   'url': 'https://us-south.ml.cloud.ibm.com',
                   'apikey':'xxx'
                  }

    client = APIClient(wml_credentials)
    client.set.default_space('xxx')

    imputed_df = df.copy()

    imputed_df['DATE'] = pd.to_datetime(imputed_df['DATE'])
    imputed_df = imputed_df.sort_values(by='DATE')

    imputed_df['DATE']=imputed_df['DATE'].dt.date
    name = imputed_df.TICKER[0]

    from sklearn.preprocessing import LabelEncoder

    temp_dict = dict()





    imputed_df.dropna(inplace=True)


    columns = imputed_df.columns
    for column in columns:

        if column=='ID':
            continue

        #impute missing values 
        # mean for numerical and 'missing' for categorical
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

    # Create a random forest regressor
    rf = RandomForestRegressor(n_estimators=200)

    X = imputed_df.drop(['FUTURE_CLOSE_PRICE'], axis=1)
    y = imputed_df['FUTURE_CLOSE_PRICE']


    rf.fit(X,y)

    sw_spec_id = client.software_specifications.get_id_by_name('default_py3.7')
    metadata = {

       client.repository.ModelMetaNames.NAME: 'rf_sklearn_wml_'+name,
       client.repository.ModelMetaNames.SOFTWARE_SPEC_UID: sw_spec_id,
       client.repository.ModelMetaNames.TYPE: 'scikit-learn_0.23'
       }
    model_dic = client.repository.store_model(rf, meta_props=metadata)
    metadata_dic = model_dic.get('metadata')
    model_id = metadata_dic.get('id')
    model_name = metadata_dic.get('name')
    self.output([model_id, model_name])

"""

output_signature = {'model_id': 'str', 'model_name': 'str'}

import time

start = time.time()

nz_groupapply = NZFunGroupedApply(df=idadf, code_str=code_str_host_spus, index='TICKER', fun_name="stocks_rf_ml",
                                  output_signature=output_signature, merge_output_with_df=False)

result = nz_groupapply.get_result()
result = result.as_dataframe()

print(result)
end = time.time()
print(end - start)

#retrieve models from wlm and score the test dataset on nps

idadf = IdaDataFrame(idadb, 'stocks_features_test')
print(idadf.head())

code_str_host_spus = """def stocks_rf_ml(self, df):

    import numpy as np


    from sklearn.impute import SimpleImputer
    from sklearn.metrics import mean_squared_error
    from sklearn.ensemble import RandomForestRegressor
    from ibm_watson_machine_learning import APIClient



    imputed_df = df.copy()
    imputed_df.dropna(inplace=True)
    name = imputed_df.TICKER[0]
    #test = str(self.getDatasliceId())+'_'+str(self.getNumberOfSpus())+'_'+str(self.getNumberOfDataSlices())

    from sklearn.preprocessing import LabelEncoder

    temp_dict = dict()



    #imputed_df.dropna(inplace=True)


    columns = imputed_df.columns
    for column in columns:

        if column=='ID':
            continue

        #impute missing values 
        # mean for numerical and 'missing' for categorical
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

    wml_credentials = {
                   'url': 'https://us-south.ml.cloud.ibm.com',
                   'apikey':'xxx'
                  }

    client = APIClient(wml_credentials)
    client.set.default_space('xxx')
    model_dict= client.repository.get_model_details()




    rf=None


    for key, value in model_dict.items():

        if (key=='resources'):
            for res_element in value:
                metadata_dic = res_element.get('metadata')
                #print(metadata_dic)
                model_name ='rf_sklearn_wml_'+name
                if (metadata_dic.get('name')==model_name):
                   rf = client.repository.load(metadata_dic.get('id'))






    X_test = imputed_df.drop(['FUTURE_CLOSE_PRICE'], axis=1)
    y_test = imputed_df['FUTURE_CLOSE_PRICE']

    test_results_df = X_test.copy()

    y_pred = rf.predict(X_test)
    test_results_df['FUTURE_CLOSE_PRICE_PRED'] =y_pred
    test_results_df['FUTURE_CLOSE_PRICE'] =y_test



    accuracy  = rf.score(X_test, y_test)    


    rms = mean_squared_error(y_test, y_pred)

    ds_size = len(X_test)
    test_results_df['DATASET_SIZE'] = ds_size
    test_results_df['ACCURACY']=round(accuracy,2)
    test_results_df['MEAN_SQUARE_ERROR']=round(rms)


    #for all the columns that had label encoders, do an inverse transform

    original_columns = test_results_df.columns

    for column in original_columns:

     if column in temp_dict:   
      test_results_df[column] = temp_dict[column].inverse_transform(test_results_df[column])








    def print_output(x):
                row = [int(x['ID']), x['FUTURE_CLOSE_PRICE_PRED'], x['DATASET_SIZE'], x['ACCURACY']]


                self.output(row)


    test_results_df.apply(print_output, axis=1)



"""

output_signature = {'ID': 'int', 'FUTURE_CLOSE_PRICE_PRED': 'double',
                    'DATASET_SIZE': 'int', 'ACCURACY': 'float'}

import time

start = time.time()

nz_groupapply = NZFunGroupedApply(df=idadf, code_str=code_str_host_spus, index='TICKER', fun_name="stocks_rf_ml",
                                  output_signature=output_signature, merge_output_with_df=True)

result = nz_groupapply.get_result()
result = result.as_dataframe()


print(result)
end = time.time()
print(end - start)

