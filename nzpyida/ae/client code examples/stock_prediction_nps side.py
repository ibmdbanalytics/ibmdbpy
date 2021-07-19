
from nzpyida import IdaDataBase, IdaDataFrame

from nzpyida.ae import NZFunGroupedApply

dsn ='bank'

idadb = IdaDataBase(dsn, 'admin', 'password')
print(idadb)


idadf = IdaDataFrame(idadb, 'stocks')
print(idadf.head())


query = 'select * from stocks'

df = idadf.ida_query(query)
print(df.dtypes)


code_str_host_spus = """def stocks_rf_ml(self, df):

    import numpy as np

    #how many days in future you need to predict
    future_days = -1

    from sklearn.impute import SimpleImputer
    from sklearn.metrics import mean_squared_error
    from sklearn.ensemble import RandomForestRegressor
    from sklearn.model_selection import train_test_split


    from sklearn.preprocessing import LabelEncoder

    temp_dict = dict()
    
    
    
    df['DATE'] = pd.to_datetime(df['DATE'])
    df = df.sort_values(by='DATE')
    df['DATE']=df['DATE'].dt.date

    # data preparation

    imputed_df = df.copy()
    
    
    # add the future close price column and shift by the required days

    imputed_df['FUTURE_CLOSE_PRICE'] = imputed_df['ADJCLOSE'].shift(future_days)



    # add technical indicators
    for n in [14,30,50,200]:

     # create the moving average indicator 
     imputed_df['ma'+str(n)] = imputed_df['ADJCLOSE'].ewm(span=n,adjust=False).mean()



    ds_size = len(imputed_df)
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

    X_train, X_test, y_train, y_test = train_test_split(X,y, test_size = 0.25, random_state=42)


    rf.fit(X_train,y_train)




    pred_df = X_test.copy()
    pred_df['FUTURE_CLOSE_PRICE'] = y_test

    y_pred = rf.predict(X_test)
    pred_df['FUTURE_CLOSE_PRICE_PRED'] =y_pred



    accuracy  = rf.score(X_test, y_test)    

    rms = mean_squared_error(y_test, y_pred)


    pred_df['DATASET_SIZE'] = ds_size
    pred_df['accuracy']=round(accuracy,2)
    pred_df['mean_square_error']=round(rms)


    #for all the columns that had label encoders, do an inverse transform

    original_columns = pred_df.columns

    for column in original_columns:

     if column in temp_dict:   
      pred_df[column] = temp_dict[column].inverse_transform(pred_df[column])

    #pred_df['DATE'] = pd.to_datetime(pred_df['DATE'])



    #pred_df = pred_df.sort_values(by='DATE')
    #pred_df['DATE']=pred_df['DATE'].dt.date

    def print_output(x):
                row = [x['ID'], x['FUTURE_CLOSE_PRICE'], x['FUTURE_CLOSE_PRICE_PRED'], x['DATASET_SIZE'], x['accuracy']]
               
                
                self.output(row)


    pred_df.apply(print_output, axis=1)



"""

output_signature= {'ID': 'float', 'FUTURE_CLOSE_PRICE': 'double', 'FUTURE_CLOSE_PRICE_PRED': 'double',
                   'DATASET_SIZE': 'int', 'ACCURACY': 'float'}


import time

start = time.time()

nz_groupapply = NZFunGroupedApply(df=idadf, code_str=code_str_host_spus, index='TICKER', fun_name="stocks_rf_ml",
                                  output_signature=output_signature, merge_output_with_df=True)

result = nz_groupapply.get_result()
result = result.as_dataframe()
print("Host+ SPUs execution - slicing on user selection -ML function for partitions within slices\n")
print(result)
end = time.time()
print(end - start)




