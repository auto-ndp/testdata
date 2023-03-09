import os

def getData(fname, key):
  foundTitle = 0
  title = []
  with open(fname, encoding='utf-8', errors='ignore') as file:
    while True:
      try:
        line = next(file)
        words = line.strip().split(' ')
        if words[0] != '' and (foundTitle == 1 or words[0] == "{key}:".format(key=key)):
          title += words
          foundTitle = 1
        elif foundTitle == 1 and words[0] == '':
          break
      except:
        return -1
  return ' '.join(title[1:])


outfile = open('corpus.txt', 'a')
for id in range(70125):
  fname = 'gutenberg/data/raw/PG{id}_raw.txt'.format(id=id)
  if not os.path.isfile(fname):
    continue
  print('\r{id}'.format(id=id), end='')
  meta = {
    'id' : id,
    'title' : getData(fname, 'Title'),
    'author' : getData(fname, 'Author'),
    'language' : getData(fname, 'Language')
  }
  if meta['title'] != -1 and meta['author'] != -1 and meta['language'] != -1:
    outfile.write(str(meta) + '\n')
