import os
import pickle
import unicodedata
import re
import time
import inspect

_cacheRoot = './.pycache'
_scope = ''
indexName = 'cacheIndex'

def slugify(value):
  """
  Normalizes string, converts to lowercase, removes non-alpha
  characters,
  and converts spaces to hyphens.

  copied from
  http://stackoverflow.com/questions/295135/turn-a-string-into-a-valid-filename-in-python
  """
  value = unicodedata.normalize('NFKD', unicode(value)).encode('ascii', 'ignore')
  value = unicode(re.sub('[^\w\s-]', '', value).strip().lower())
  value = unicode(re.sub('[-\s]+', '-', value))
  return str(value)

def setCacheRoot(cacheRoot):
  '''sets the path used to store cached results'''
  _cacheRoot = cacheRoot

def touchPath(scope, cacheRoot):
  os.makedirs(os.path.join(cacheRoot,scope))

def setDefaultScope(scope):
  _scope = scope

def getCacheKey(function, arguments):
  return slugify(function.__name__ +'::'+str(arguments))

def getTimestamp():
  return int(time.time()*1000000000)

def getCacheFileName(function, arguments, timestamp):
  return getCacheKey(function, arguments) \
    + '::' + str(timestamp)+'.cache'

def loadIndex(scope, cacheRoot):
  touchPath(scope, cacheRoot)
  try:
    indexFile = open(os.path.join(cacheRoot+scope,indexName))
    index = pickle.load(indexFile)
    indexFile.close()
  except IOError:
    index = {}
    writeIndex(index, scope, cacheRoot)
  return index

def writeIndex(index, scope, cacheRoot):
  indexFile = open(os.path.join(cacheRoot+scope, indexName), 'w')
  pickle.dump(index, indexFile)
  indexFile.close()

def checkCache(function, arguments, scope= _scope, cacheRoot= _cacheRoot):
  index = loadIndex(scope, cacheRoot)

  cacheKey = getCacheKey(function, arguments)

  if cacheKey not in index:
    return {'cacheFile': None, 'logFiles': []}

  return index[cacheKey]

def getCacheFile(function, arguments, scope, cacheRoot):
  return checkCache(function, arguments, scope, cacheRoot)['cacheFile']

def getLogFiles(function, arguments, filterFunc, scope, cacheRoot):
  return filter(checkCache(function, arguments, scope, cacheRoot)['logFiles'], filterFunc)

def blankIndexEntry():
  return {'cacheFile': None, 'cacheTime': 0, 'logFiles': []}

def addToIndex(cacheKey, metaData, timestamp, index, cacheFile, setCache):
  '''
  adds an entry corresponding to cacheKey with timestamp and metaData to an index
  dictionary.
  setCache is a true/false flag indicated whether the new entry is only a
  write-only log or can be accessed as a cache-hit on a lookup.
  CAREFUL: THIS FUNCTION MODIFIES THE SUPPLIED INDEX DICTIONARY
  '''
  if cacheKey not in index:
    index[cacheKey] = blankIndexEntry()

  index[cacheKey]['logFiles'].append({'fileName': cacheFile, 'time': timestamp, 'metaData': metaData})
  if setCache and index[cacheKey]['cacheTime'] < timestamp:
    index[cacheKey]['cacheFile'] = cacheFile
    index[cacheKey]['cacheTime'] = timestamp

def writeEntryToIndex(function, arguments, metaData, timestamp, cacheFile, setCache, \
    scope, cacheRoot):
  index = loadIndex(scope, cacheRoot)
  cacheKey = getCacheKey(function, arguments)
  addToIndex(cacheKey, metaData, timestamp, index, cacheFile, setCache)
  writeIndex(index, scope, cacheRoot)

def rebuildIndex(scope, cacheRoot):
  '''
  scans files in a directory to recover the index in case the index is
  corrupted.

  the index is a dictionary whose keys are 'cacheKeys' (filenames)
  The value for a cacheKey is in turn a dictionary containing the key
  'logFiles' whose value is a list of files listed under this cache key,
  'cacheFile' is the file to return on a cache hit
  'timestampt' is the time of cacheFile was created. 
  See 'addToIndex' function for a codified description of this.
  '''
  path = os.path.join(cacheRoot,scope)
  fileNames = [f for f in os.listdir(path) if \
      os.path.isfile(os.path.join(path,f))]

  index = {}
  for f in fileNames:
    try:
      fp = open(f)
      cacheData = pickle.load(fp)
      cacheKey = cacheData['cacheKey']
      isCacheHit = cacheData['isCacheHit']
      timestamp = cacheData['timestamp']
      addToIndex(cacheKey, timestamp, index, f, isCacheHit)
    except:
      pass
  writeIndex(index, scope, cacheRoot)

def getResultsFromCacheFile(cacheFile, scope, cacheRoot):
  path = os.path.join(cacheRoot, scope, cacheFile)
  fp = open(path)
  cacheData = pickle.load(fp)
  fp.close()
  return cacheData['results']

def writeDataToCacheFile(cacheData, cacheFile, scope, cacheRoot):
  path = os.path.join(cacheRoot, scope, cacheFile)
  fp = open(path, 'w')
  pickle.dump(cacheData, fp)
  fp.close()

def cache(function, arguments, scope = _scope, cacheRoot = _cacheRoot):
  '''
  caches the results of running a function with keyword arguments specified
  by the dictionary arguments in a given scope from the cacheRoot.
  Note that you should be very careful about caching functions that have
  side-effects as recovering the function results from cache will not
  re-execute side effects.
  '''

  cacheFile  = getCacheFile(function, arguments, scope, cacheRoot)
  if cacheFile!=None:
    return getResultsFromCacheFile(cacheFile, scope, cacheRoot)

  return log(function, arguments, True, scope, cacheRoot)

def log(function, arguments, metaData=None, cache = True, scope = _scope, cacheRoot= _cacheRoot):
  '''
  runs the function on the arguments and stores the restult in a logfile.
  These results can be recalled as a cached result of the function later if
  cache = True.
  Notice that this ALWAYS runs the function, regardless of whether it has been
  cached already.

  metaData is an object that is stored in the metaData section of the cache index. It should
  be a small object used for filtering log files.
  '''

  cacheKey = getCacheKey(function, arguments)
  cacheData = {}
  cacheData['cacheKey'] = cacheKey
  cacheData['isCacheHit'] = True
  cacheData['function'] = function.__name__
  cacheData['arguments'] = arguments
  cacheData['results'] = function(**arguments)
  timestamp = getTimestamp()
  cacheData['timestamp'] = timestamp
  cacheData['metaData'] = metaData

  cacheFile = getCacheFileName(function, arguments, timestamp)

  writeDataToCacheFile(cacheData, cacheFile, scope, cacheRoot)
  writeEntryToIndex(function, arguments, metaData, timestamp, cacheFile, True, scope, \
      cacheRoot)
  return cacheData['results']

def cachify(function, scope = _scope, cacheRoot = _cacheRoot):
  argsList = inspect.getargspec(function).args
  def cachifiedFunction(*args, **kwargs):
    argsDict = dict(zip(argsList,args))
    argsDict.update(kwargs)
    cache(function, argsDict, scope, cacheRoot)
  return cachifiedFunction

def logify(function, cache = True, scope = _scope, cacheRoot = _cacheRoot):
  argsList = inspect.getargspec(function).args
  def logifiedFunction(*args, **kwargs):
    argsDict = dict(zip(argsList,args))
    argsDict.update(kwargs)
    log(function, argsDict, cache, scope, cacheRoot)
  return cachifiedFunction

def getSaveFunc(data):
  def saveData(title):
    return {'data': data, 'title': title}
  return saveData

def save(data, title, metaData=None, scope = _scope, cacheRoot = _cacheRoot):
  saveFunc = getSaveFunc(data)
  arguments = {'title': title}
  log(saveFunc, {'title': title}, metaData)


def get(data, title, filterFunc = lambda x: True, scope = _scope, cacheRoot = _cacheRoot):
  saveFunc = getSaveFunc(data)
  arguments = {'title': title}

  logFiles = getLogFiles(saveFunc, arguments, filterFunc, scope, cacheRoot)

  return [{'savedData': getResultsFromCacheFile(logFile['fileName']), 'metaData': logFile['metaData']} for logFile in logFiles]
