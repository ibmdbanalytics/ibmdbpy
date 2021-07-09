from nzpyida import IdaDataBase, IdaDataFrame
from nzpyida.ae import NZFunTApply
from nzpyida.ae import NZFunApply
from nzpyida.ae import NZFunGroupedApply
dsn = "jdbc:netezza://150.239.60.252:5480/histdbi"

dsn ={
    "database":"histdbi",
     "port" :5480,
     "host" : "150.239.60.252",
     "securityLevel":0,
     "logLevel":0


}

idadb = IdaDataBase(dsn, uid="admin", pwd="Ex9tdoavhMix4mi3XwLVPxJa")
idadf = IdaDataFrame(idadb, 'NZ_QUERY_HISTORY')
code_str_apply = """def apply_fun(self, x):
    max_temp = x[27]
    fahren_max_temp = (max_temp*1.8)+32
    row = [max_temp,  fahren_max_temp]
    self.output(row)
    """
output_signature = {'MAX_TEMP': 'float', 'FAHREN_MAX_TEMP': 'float'}
nz_apply = NZFunApply(df=idadf, code_str=code_str_apply, fun_name='apply_fun', output_table="temp_conversion",output_signature=output_signature, merge_output_with_df=True)
result = nz_apply.get_result()
print(result)