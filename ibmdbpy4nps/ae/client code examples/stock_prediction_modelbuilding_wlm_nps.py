


from ibmdbpy4nps import IdaDataBase, IdaDataFrame
from ibmdbpy4nps.ae import NZFunGroupedApply
from ibm_watson_machine_learning import APIClient

dsn = 'bank'

idadb = IdaDataBase(dsn, 'admin', 'password')

idadf = IdaDataFrame(idadb, 'stocks')

print(idadf.head())


def delete_models():

 from ibm_watson_machine_learning import APIClient

 wml_credentials = {
                   'url': 'https://us-south.ml.cloud.ibm.com',
                   'apikey':'xxx'
                  }

 client = APIClient(wml_credentials)
 client.set.default_space('xxx')
 #delete the previous models
 model_dict = client.repository.get_model_details()
 client.repository.delete

 for key, value in model_dict.items():

    if (key == 'resources'):
        for res_element in value:
            metadata_dic = res_element.get('metadata')
            res = client.repository.delete(metadata_dic.get('id'))
            print(res)




#delete the previous models
delete_models()


code_str_host_spus = """def stocks_rf_ml(self, df):

    import numpy as np

    from sklearn.impute import SimpleImputer
    from sklearn.metrics import mean_squared_error
    from sklearn.ensemble import RandomForestRegressor
    from ibm_watson_machine_learning import APIClient
    from sklearn.model_selection import train_test_split
    
    wml_credentials = {
                   'url': 'https://us-south.ml.cloud.ibm.com',
                   'apikey':'xxxx'
                  }

    client = APIClient(wml_credentials)
    client.set.default_space('xxxx')

    imputed_df = df.copy()
    
    imputed_df['DATE'] = pd.to_datetime(imputed_df['DATE'])
    imputed_df = imputed_df.sort_values(by='DATE')
     
    imputed_df['DATE']=imputed_df['DATE'].dt.date
    name = imputed_df.TICKER[0]
    
    from sklearn.preprocessing import LabelEncoder

    temp_dict = dict()
    
    #how many days in future you need to predict
    future_days = -1
    
    #add the future close price column and shift by the required days
    
    imputed_df['FUTURE_CLOSE_PRICE'] = imputed_df['ADJCLOSE'].shift(future_days)

    
    
    # add technical indicators
    for n in [14,30,50,200]:

     # create the moving average indicator 
     imputed_df['MA'+str(n)] = imputed_df['ADJCLOSE'].rolling(window=n).mean()
    
    
  

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
    
    # Create a random forest regressor
    rf = RandomForestRegressor(n_estimators=200)
    
    X = imputed_df.drop(['FUTURE_CLOSE_PRICE'], axis=1)
    y = imputed_df['FUTURE_CLOSE_PRICE']
    #print(y)
    X_train, X_test, y_train, y_test = train_test_split(X,y, test_size = 0.25, random_state=42)
      
    rf.fit(X_train,y_train)
   
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
print("Host+ SPUs execution - slicing on user selection -ML function for partitions within slices\n")

print(result)
end = time.time()
print(end - start)





