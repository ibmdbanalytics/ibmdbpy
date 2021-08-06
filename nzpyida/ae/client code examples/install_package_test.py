from nzpyida import IdaDataBase, IdaDataFrame

from nzpyida.ae.install import NZInstall

idadb = IdaDataBase('fyre', 'admin', 'password')

nzinstall = NZInstall(idadb, package_name='pandas')
print(nzinstall.getResultCode())

