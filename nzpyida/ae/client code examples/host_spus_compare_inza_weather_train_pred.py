

from nzpyida import IdaDataBase, IdaDataFrame

from nzpyida.ae import NZFunTApply
from nzpyida.ae import NZFunApply
from nzpyida.ae import NZFunGroupedApply


#jdbc dsn
#dsn = "jdbc:netezza://xxxx:5480/weather"





#nzpy dsn
dsn ={
    "database":"weather",
     "port" :5480,
     "host" : "xxxx",
     "securityLevel":0,
     "logLevel":0


}

#odbc dsn
dsn='fyre'
idadb = IdaDataBase(dsn, 'admin', 'password',verbose=True)



#query = "select * from weather_new limit 1000"
print(idadb)

idadf = IdaDataFrame(idadb, 'WEATHER')
print(idadf.dtypes)

#query = 'select * from weather limit 10000'




#df = idadf.ida_query(query)


code_str_host = """def decision_tree_ml_host(self, df):

    from sklearn.model_selection import cross_val_score
    from sklearn.impute import SimpleImputer
    from sklearn.tree import DecisionTreeClassifier
    from sklearn.model_selection import train_test_split
    import datetime

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
                    imputed_df[column] = imputed_df[column].astype(str)
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
            #test_date = pd.to_datetime(pred_df['DATE'])
            #pred_df['TEST_DATE'] = pd.to_datetime('2020-10-01') 
            #pred_df['TEST_DATE']= pred_df['TEST_DATE'].dt.date
            pred_df['TEST_DATE'] =10
            
            
           
            
            
            




            original_columns = pred_df.columns

            for column in original_columns:

             if column in temp_dict:   
               pred_df[column] = temp_dict[column].inverse_transform(pred_df[column])
               #print(pred_df)

            def print_output(x):
                row = [x['ID'], x['TEST_DATE'],  x['RAINTOMORROW'], x['DATASET_SIZE'], x['CLASSIFIER_ACCURACY']]
                
                self.output(row)


            pred_df.apply(print_output, axis=1)
                      
            return pred_df
            
 

        ml_result = decision_tree_classifier(df=group)

        """





output_signature = {'ID':'int', 'TEST_DATE':'date',  'RAINTOMORROW_PRED' :'bool',  'DATASET_SIZE':'int', 'CLASSIFIER_ACCURACY':'float'}


import time
start = time.time()

nz_tapply = NZFunTApply(df=idadf, code_str=code_str_host, fun_name='decision_tree_ml_host', parallel=False,  output_signature=output_signature, merge_output_with_df=True)
result_idadf = nz_tapply.get_result()
result_df = result_idadf.as_dataframe()
idadb.drop_table(result_idadf.tablename)
print("\n")
#pd.set_option('display.max_columns', None)
#pd.set_option('display.width', None)
#pd.set_option('display.max_rows', None)
#pd.set_option('display.max_colwidth', -1)
print(result_df)
end = time.time()
print(end - start)

groups = result_df.groupby("LOCATION")
for name, group in groups:
    print(name + ":" + str(len(group)))


code_str_apply="""
def apply_fun(self, x):
    from math import sqrt
    max_temp = x[1]
    id = x[0]
    fahren_max_temp = (max_temp*1.8)+32
    row = [id, max_temp,  fahren_max_temp]
    self.output(row)"""
output_signature = {'ID':'int', 'RESULT_MAX_TEMP' :'float', 'RESULT_FAHREN_MAX_TEMP' : 'float'}
nz_apply = NZFunApply(df=idadf, code_str= code_str_apply, fun_name="apply_fun", columns=['ID', 'MAXTEMP'], output_signature=output_signature, merge_output_with_df=True)
result = nz_apply.get_result()
result=result.as_dataframe()
print(result)



end = time.time()
print(end - start)


code_str_host_spus="""def decision_tree_ml(self, df):
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


output_signature = {'ID':'int', 'RAINTOMORROW_PRED' :'str',  'DATASET_SIZE':'int', 'CLASSIFIER_ACCURACY':'float'}
import time
start = time.time()

nz_groupapply = NZFunGroupedApply(df=idadf,  code_str=code_str_host_spus, index='LOCATION', fun_name="decision_tree_ml", output_signature=output_signature, merge_output_with_df=True)
#nz_groupapply = NZFunGroupedApply(df=idadf,  code_str=code_str_host_spus, index='LOCATION', fun_name="decision_tree_ml")
result = nz_groupapply.get_result()

print("Host+ SPUs execution - slicing on user selection -ML function for partitions within slices\n")
print(result)
groups = result.as_dataframe().groupby("LOCATION")
for name, group in groups:
    print (name +":"+str(len(group)))
end = time.time()
print(end - start)



import time
start = time.time()
output_signature = {'ID':'int', 'RAINTOMORROW_PRED' :'bool',  'DATASET_SIZE':'int', 'CLASSIFIER_ACCURACY':'float'}
nz_groupapply = NZFunTApply(df=idadf, code_str=code_str_host_spus, fun_name ="decision_tree_ml", parallel=True, output_signature=output_signature)
result = nz_groupapply.get_result()
result = result.as_dataframe()
print("Host +SPUs execution - slicing on a default column- ML function for the entire slices")
print(result)
print("\n")

end = time.time()
print(end - start)