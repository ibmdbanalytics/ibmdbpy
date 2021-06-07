
import pandas as pd

from ibmdbpy4nps import IdaDataBase, IdaDataFrame
from ibmdbpy4nps.ae import NZFunGroupedApply

dsn = 'bank'

idadb = IdaDataBase(dsn, 'admin', 'password')

idadf = IdaDataFrame(idadb, 'stocks_test')

print(idadf.head())
print(idadf.dtypes)

code_str_host_spus = """def stocks_rf_ml(self, df):

    import numpy as np


    from sklearn.impute import SimpleImputer
    from sklearn.metrics import mean_squared_error
    from sklearn.ensemble import RandomForestRegressor
    from ibm_watson_machine_learning import APIClient



    imputed_df = df.copy()
    name = imputed_df.TICKER[0]
    test = str(self.getDatasliceId())+'_'+str(self.getNumberOfSpus())+'_'+str(self.getNumberOfDataSlices())

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
                   'apikey':'iPlcsL-Cw18TgaAwk_mhFYkwwr5MDa9zLq0hhNXrSQpR'
                  }

    client = APIClient(wml_credentials)
    client.set.default_space('48a32d7c-bc0d-4abf-b396-63bf35ed76c1')
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
                row = [int(x['ID']), x['FUTURE_CLOSE_PRICE_PRED'], test, x['DATASET_SIZE'], x['ACCURACY']]


                self.output(row)


    test_results_df.apply(print_output, axis=1)



"""

output_signature = {'ID': 'int', 'FUTURE_CLOSE_PRICE_PRED': 'double', 'TEST': 'str',
                    'DATASET_SIZE': 'int', 'ACCURACY': 'float'}

import time

start = time.time()

nz_groupapply = NZFunGroupedApply(df=idadf, code_str=code_str_host_spus, index='TICKER', fun_name="stocks_rf_ml",
                                  output_signature=output_signature, merge_output_with_df=True)

result = nz_groupapply.get_result()
result = result.as_dataframe()
print("Host+ SPUs execution - slicing on user selection -ML function for partitions within slices\n")
print(result[['ID']])
print(result)
end = time.time()
print(end - start)

test_pred_groups = result.groupby('TICKER')

for name, group in test_pred_groups:
    input = group.copy()
    input["DATE"] = pd.to_datetime(input["DATE"])

    input = input.sort_values(by="DATE")
    input['DATE'] = input['DATE'].dt.date
    print(input)

    # sns.lineplot(data=result_df, hue="TICKER", x="DATE", y="future_close_price_pred")
    #input.plot(x="DATE", y=["FUTURE_CLOSE_PRICE", "FUTURE_CLOSE_PRICE_PRED", ], kind="line", title=name)



