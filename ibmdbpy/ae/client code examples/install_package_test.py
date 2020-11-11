from ibmdbpy import IdaDataBase, IdaDataFrame

from ibmdbpy.ae.install import NZInstall


nzinstall = NZInstall(package_name='scikit-learn')
print(nzinstall.getResultCode())

