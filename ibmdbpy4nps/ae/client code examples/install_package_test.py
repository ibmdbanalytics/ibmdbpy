from ibmdbpy4nps import IdaDataBase, IdaDataFrame

from ibmdbpy4nps.ae.install import NZInstall


nzinstall = NZInstall(package_name='scikit-learn')
print(nzinstall.getResultCode())

