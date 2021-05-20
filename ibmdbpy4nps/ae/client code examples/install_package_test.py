from ibmdbpy4nps import IdaDataBase, IdaDataFrame

from ibmdbpy4nps.ae.install import NZInstall

idadb = IdaDataBase('weather', 'admin', 'password')

nzinstall = NZInstall(idadb, package_name='pandas')
print(nzinstall.getResultCode())

