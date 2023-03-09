import os

for file in os.listdir('/data'):
  filename = '/data/' + file
  id = int(file[2:-8])
  cmd = "rados put -p ndp f{id} {fname}".format(id=id, fname=filename)
  os.system(cmd)