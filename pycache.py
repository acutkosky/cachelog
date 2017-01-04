"""
Cache the output from expensive functions on disk.
"""
import os
import pickle
import unicodedata
import re
import time
import inspect
import random

CACHE_ROOT = '.pycache'
SCOPE = ''
LOCK_DIR = '.locks'
INDEX_NAME = 'cacheIndex'

LOCK_INDEX_PREFIX = 'lock_index'

VERSION = 0.1

ID = int(random.random()*10000000000000000)

def slugify(value):
    """
    Normalizes string, converts to lowercase, removes non-alpha
    characters,
    and converts spaces to hyphens.

    copied from
    http://stackoverflow.com/questions/295135/turn-a-string-into-a-valid-filename-in-python
    """
    value = unicodedata.normalize('NFKD', unicode(value)).encode('ascii', 'ignore')
    value = unicode(re.sub(r'[^\w\s-]', '', value).strip().lower())
    value = unicode(re.sub(r'[-\s]+', '-', value))
    return str(value)

def set_cache_root(cache_root):
    '''sets the path used to store cached results'''
    CACHE_ROOT = cache_root

def touch_path(scope, cache_root):
    '''creates directories for given scope'''
    try:
        os.makedirs(os.path.join(cache_root, scope, LOCK_DIR))
    except OSError:
        pass

def set_default_scope(scope):
    '''sets default scope variable'''
    SCOPE = scope

def get_cache_key(function, arguments):
    '''converts a function and arguments into a key used to store its output'''
    return slugify(function.__name__ +'::'+str(arguments))

def get_timestamp():
    '''gets current time'''
    return int(time.time()*1000000000)

def get_cachefile_name(function, arguments, timestamp):
    '''gets the name of the file that will hold the output of function(arguments)
    run at time timestamp'''
    return get_cache_key(function, arguments) \
        + '::' + str(timestamp)+'.cache'

def get_lockfile_name(scope, cache_root):
    '''gets the name of file used to claim the lock on the cache index'''
    return os.path.join(cache_root, scope, LOCK_DIR, LOCK_INDEX_PREFIX + ':' + str(ID))

def get_indexlock_holder(scope, cache_root):
    '''finds the ID of the process that currently holds the lock on the cache index'''
    path = os.path.join(cache_root, scope, LOCK_DIR)
    file_names = [f for f in os.listdir(path) if \
        os.path.isfile(os.path.join(path, f)) and f.find(LOCK_INDEX_PREFIX) == 0]

    def get_time_id_pairs(file_names):
        '''helper function for extracting the list of processes (by ID) waiting on the lock
        and when each ID asked for the lock.'''
        for file_name in file_names:
            file_pointer = open(os.path.join(path, file_name))
            try:
                time_id_pair = pickle.load(file_pointer)
            except (EOFError, ValueError):
                time_id_pair = None
            file_pointer.close()
            if time_id_pair != None:
                yield time_id_pair

    time_id_pairs = [pair for pair in get_time_id_pairs(file_names)]

    if len(time_id_pairs) == 0:
        return -1

    min_time_stamp = min([x['timestamp'] for x in time_id_pairs])
    lock_holder = min([x['ID'] for x in time_id_pairs if x['timestamp'] == min_time_stamp])

    return lock_holder

def index_locked_by_us(scope, cache_root):
    '''returns true if this process holds the lock on the cache index.'''
    return get_indexlock_holder(scope, cache_root) == ID

def unlock_index(scope, cache_root):
    '''release the lock on the cache index.'''
    try:
        lockfile_name = get_lockfile_name(scope, cache_root)
        os.remove(lockfile_name)
    except OSError:
        pass

def lock_index(scope, cache_root):
    '''acquire the lock on the cache index.
    Returns when the lock has been acquired.

    BUSY WAITS FOR NOW - IN FUTURE THIS SHOULD PROBABLY BECOME ASYNC
    '''

    #Prevent double-locking
    assert not index_locked_by_us(scope, cache_root)

    lockfile_name = get_lockfile_name(scope, cache_root)
    lockfile = open(lockfile_name, 'w')
    timestamp = get_timestamp()
    pickle.dump({'timestamp': timestamp, 'ID': ID}, lockfile)
    lockfile.close()

    while not index_locked_by_us(scope, cache_root):
        pass

def load_index(scope, cache_root):
    '''loads the index of cache entries

    Must hold index lock to load the index'''

    touch_path(scope, cache_root)
    assert index_locked_by_us(scope, cache_root)
    try:
        indexfile = open(os.path.join(cache_root, scope, INDEX_NAME))
        index = pickle.load(indexfile)
        indexfile.close()
    except IOError:
        index = {}
        write_index(index, scope, cache_root)
    return index

def write_index(index, scope, cache_root):
    '''saves current copy of cache index in memory to disk.

    Must hold the index lock to do this.'''

    assert index_locked_by_us(scope, cache_root)
    indexfile = open(os.path.join(cache_root+scope, INDEX_NAME), 'w')
    pickle.dump(index, indexfile)
    indexfile.close()

def check_cache(function, arguments, scope=SCOPE, cache_root=CACHE_ROOT):
    '''search cached data for an entry corresponding to function(arguments)

    Must hold index lock in this function.'''

    index = load_index(scope, cache_root)

    cache_key = get_cache_key(function, arguments)

    if cache_key not in index:
        return {'cache_file': None, 'logfiles': []}

    return index[cache_key]

def get_cache_file(function, arguments, scope, cache_root):
    '''searches cache for an entry corresponding to function(arguments)

    takes care of necessary locking.
    '''
    lock_index(scope, cache_root)
    cache_file = check_cache(function, arguments, scope, cache_root)['cache_file']
    unlock_index(scope, cache_root)
    return cache_file

def get_logfiles(function, arguments, filter_func, scope, cache_root):
    '''finds all cache entries tagged as "logs" for function(arguments)'''
    return filter_func(check_cache(function, arguments, scope, cache_root)['logfiles'])

def blank_index_entry():
    '''generates an empty cache index entry to be filled in'''
    return {'cache_file': None, 'cacheTime': 0, 'logfiles': []}

def add_to_index(cache_key, metadata, timestamp, index, cache_file, setcache_flag):
    '''
    adds an entry corresponding to cache_key with timestamp and metadata to an index
    dictionary.
    setcache_flag is a true/false flag indicated whether the new entry is only a
    write-only log or can be accessed as a cache-hit on a lookup.
    CAREFUL: THIS FUNCTION MODIFIES THE SUPPLIED INDEX DICTIONARY
    '''
    if cache_key not in index:
        index[cache_key] = blank_index_entry()

    index[cache_key]['logfiles'].append({'file_name': cache_file, 'time': timestamp, \
        'metadata': metadata})
    if setcache_flag and index[cache_key]['cacheTime'] < timestamp:
        index[cache_key]['cache_file'] = cache_file
        index[cache_key]['cacheTime'] = timestamp

def write_entry_to_index(function, arguments, metadata, timestamp, cache_file, setcache_flag, \
        scope, cache_root):
    '''updates the cache index to include a newly-added cached function result'''
    lock_index(scope, cache_root)
    index = load_index(scope, cache_root)
    cache_key = get_cache_key(function, arguments)
    add_to_index(cache_key, metadata, timestamp, index, cache_file, setcache_flag)
    write_index(index, scope, cache_root)
    unlock_index(scope, cache_root)

def rebuild_index(scope, cache_root):
    '''
    scans files in a directory to recover the index in case the index is
    corrupted.

    the index is a dictionary whose keys are 'cache_keys' (filenames)
    The value for a cache_key is in turn a dictionary containing the key
    'logfiles' whose value is a list of files listed under this cache key,
    'cache_file' is the file to return on a cache hit
    'timestamp' is the time of cache_file was created.
    See 'add_to_index' function for a codified description of this.
    '''
    path = os.path.join(cache_root, scope)
    file_names = [f for f in os.listdir(path) if \
            os.path.isfile(os.path.join(path, f))]

    index = {}
    for file_name in file_names:
        try:
            file_pointer = open(os.path.join(path, file_name))
            cache_data = pickle.load(file_pointer)
            file_pointer.close()
            cache_key = cache_data['cache_key']
            metadata = cache_data['metadata']
            is_cache_hit = cache_data['is_cache_hit']
            timestamp = cache_data['timestamp']
            add_to_index(cache_key, metadata, timestamp, index, file_name, is_cache_hit)
        except:
            pass
    write_index(index, scope, cache_root)

def get_results_from_cache_file(cache_file, scope, cache_root):
    '''extracts function output from cached data'''
    path = os.path.join(cache_root, scope, cache_file)
    file_pointer = open(path)
    cache_data = pickle.load(file_pointer)
    file_pointer.close()
    return cache_data['results']

def write_data_to_cache_file(cache_data, cache_file, scope, cache_root):
    '''writes function output to a given cache file.'''
    path = os.path.join(cache_root, scope, cache_file)
    file_pointer = open(path, 'w')
    pickle.dump(cache_data, file_pointer)
    file_pointer.close()

def cache(function, arguments, scope=SCOPE, cache_root=CACHE_ROOT):
    '''
    caches the results of running a function with keyword arguments specified
    by the dictionary arguments in a given scope from the cache_root.
    Note that you should be very careful about caching functions that have
    side-effects as recovering the function results from cache will not
    re-execute side effects.
    '''

    cache_file = get_cache_file(function, arguments, scope, cache_root)
    if cache_file != None:
        return get_results_from_cache_file(cache_file, scope, cache_root)

    return log(function, arguments, None, True, scope, cache_root)

def log(function, arguments, metadata=None, use_as_cache=True, scope=SCOPE, cache_root=CACHE_ROOT):
    '''
    runs the function on the arguments and stores the restult in a logfile.
    These results can be recalled as a cached result of the function later if
    use_as_cache = True.
    Notice that this ALWAYS runs the function, regardless of whether it has been
    cached already.

    metadata is an object that is stored in the metadata section of the cache index. It should
    be a small object used for filtering log files.
    '''

    touch_path(scope, cache_root)

    cache_key = get_cache_key(function, arguments)
    cache_data = {}
    cache_data['cache_key'] = cache_key
    cache_data['is_cache_hit'] = use_as_cache
    cache_data['function'] = function.__name__
    cache_data['arguments'] = arguments
    cache_data['results'] = function(**arguments)
    timestamp = get_timestamp()
    cache_data['timestamp'] = timestamp
    cache_data['metadata'] = metadata
    cache_data['pycacheversion'] = VERSION

    cache_file = get_cachefile_name(function, arguments, timestamp)

    write_data_to_cache_file(cache_data, cache_file, scope, cache_root)
    write_entry_to_index(function, arguments, metadata, timestamp, cache_file, True, scope, \
            cache_root)
    return cache_data['results']

def cachify(function, scope=SCOPE, cache_root=CACHE_ROOT):
    '''returns a wrapped version of a supplied function
    that will check for and return a cached result when called
    and store results in the cache if no cached result is available.'''

    args_list = inspect.getargspec(function).args
    def cachified_function(*args, **kwargs):
        '''cachified version of a function'''
        args_dict = dict(zip(args_list, args))
        args_dict.update(kwargs)
        cache(function, args_dict, scope, cache_root)
    cachified_function.__doc__ = function.__doc__ + '\n**** cachified ****'

    return cachified_function

def logify(function, use_as_cache=True, scope=SCOPE, cache_root=CACHE_ROOT):
    '''returns a wrapped version of a supplied function
    that will ALWAYS run the function and store the result
    in the cache with the "log" tag.'''
    args_list = inspect.getargspec(function).args
    def logified_function(*args, **kwargs):
        '''logified vesion of a function'''
        args_dict = dict(zip(args_list, args))
        args_dict.update(kwargs)
        log(function, args_dict, None, use_as_cache, scope, cache_root)
    logified_function.__doc__ = function.__doc__ + '\n**** logified ****'
    return logified_function

def get_save_func(data):
    '''returns a function that can be used with the cache machinery
    to save some arbitrary data'''
    def save_data(title):
        '''returns pre-defined data under a  supplied title'''
        return {'data': data, 'title': title}
    return save_data

def save(data, title, metadata=None, scope=SCOPE, cache_root=CACHE_ROOT):
    '''store some given data in the cache with a given title.'''
    save_func = get_save_func(data)
    arguments = {'title': title}
    log(save_func, arguments, metadata, False, scope, cache_root)

def get(title, filter_func=lambda x: x, scope=SCOPE, cache_root=CACHE_ROOT):
    '''finds all data stored under a given title, filtering the results using filter_func'''
    save_func = get_save_func(None)
    arguments = {'title': title}

    logfiles = get_logfiles(save_func, arguments, filter_func, scope, cache_root)

    return [{'savedData': get_results_from_cache_file(logFile['file_name'], scope, cache_root), \
        'metadata': logFile['metadata']} for logFile in logfiles]
