from nzpyida import IdaDataBase, IdaDataFrame


from nzpyida.ae import NZFunGroupedApply


#
#nzpy dsn
dsn ={
    "database":"customer_churn",
     "port" :5480,
     "host" : "xxxx",
     "securityLevel":0,
     "logLevel":3


}



idadb = IdaDataBase(dsn, 'admin', 'password', verbose=True )


print(idadb)

idadf = IdaDataFrame(idadb, 'customer_churn')


print(idadf.dtypes)
code_str_host_spus = """def log_reg_ml(self, df):
            from sklearn.model_selection import cross_val_score
            from sklearn.impute import SimpleImputer
            from sklearn.linear_model import LogisticRegression
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
            lr = LogisticRegression()
            X = imputed_df.drop(['EXITED'], axis=1)
            y = imputed_df['EXITED']
            X_train, X_test, y_train, y_test = train_test_split(X,y, test_size = 0.25, random_state=42, stratify=y)


            lr.fit(X_train, y_train)

            accuracy = lr.score(X_test, y_test)    
            #print(accuracy)



            pred_df = X_test.copy()


            y_pred= (lr.predict_proba(X_test)[:,1]>=0.2)

            pred_df['EXITED'] = y_pred
            pred_df['DATASET_SIZE'] = ds_size
            pred_df['CLASSIFIER_ACCURACY']=round(accuracy,2)
            




            original_columns = pred_df.columns

            for column in original_columns:

             if column in temp_dict:   
               pred_df[column] = temp_dict[column].inverse_transform(pred_df[column])
               #print(pred_df)

            def print_output(x):
                row = [x['CUSTOMERID'],x['EXITED'], x['DATASET_SIZE']]
                self.output(row)


            pred_df.apply(print_output, axis=1)






"""

output_signature = {'CUSTOMERID':'float', 'EXITED_PRED': 'int',  'DATASET_SIZE': 'int'}
import time

start = time.time()

nz_groupapply = NZFunGroupedApply(df=idadf, code_str=code_str_host_spus, index='GEOGRAPHY', fun_name="log_reg_ml",
                                  output_signature=output_signature, merge_output_with_df=True, id='CUSTOMERID')
# nz_groupapply = NZFunGroupedApply(df=idadf,  code_str=code_str_host_spus, index='LOCATION', fun_name="decision_tree_ml")
result = nz_groupapply.get_result()
result = result.as_dataframe()
print("Host+ SPUs execution - slicing on user selection -ML function for partitions within slices\n")
print(result)
end = time.time()
print(end - start)