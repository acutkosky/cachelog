"""
Cache the output from expensive functions on disk.
"""
import os
import pickle
import unicodedata
import re
import time
import inspect
import subprocess

import pymutex

DEFAULT_CACHE_ROOT = './.cachelog'
DEFAULT_SCOPE = ''
INDEX_NAME = 'cacheIndex'

VERSION = 0.1

def slugify(value):
    """
    Normalizes string, converts to lowercase, removes non-alpha
    characters,
    and converts spaces to hyphens.

    (mostly) copied from
    http://stackoverflow.com/questions/295135/turn-a-string-into-a-valid-filename-in-python
    """
    value = unicodedata.normalize('NFKD', unicode(value)).encode('ascii', 'ignore')
    value = unicode(re.sub(r'[^\w\s:-{}\[\]]', '', value).strip().lower())
    value = unicode(re.sub(r'[-\s]+', '-', value))
    return str(value)

def force_git_commit():
    '''sets the FORCE_GIT_COMMIT flag.
    If true, then disallows use with uncommitted code.'''
    if not is_committed():
        raise RuntimeError( \
    'You have uncommitted changes! For sane logging, commit all changes before saving output.')

def get_git_hash():
    '''gets the current git hash'''
    return subprocess.check_output(['git', 'rev-parse', 'HEAD']).strip()

def is_committed():
    '''checks if current code has been committed with git'''
    return subprocess.check_output(['git', 'diff']) == ''

def set_cache_root(cache_root):
    '''sets the path used to store cached results'''
    global DEFAULT_CACHE_ROOT
    DEFAULT_CACHE_ROOT = cache_root

def touch_path(scope, cache_root):
    '''creates directories for given scope'''
    try:
        os.makedirs(os.path.join(cache_root, scope))
    except OSError:
        pass

def set_default_scope(scope):
    '''sets default scope variable'''
    global DEFAULT_SCOPE
    DEFAULT_SCOPE = scope

def get_func_name(function):
    '''if the argument is a function object, returns its name.
    otherwise assumes it is a string corresponding to the name
    of a function.'''

    if hasattr(function,'__name__'):
        return function.__name__
    else:
        return function   

def get_cache_key(function, arguments):
    '''converts a function and arguments into a key used to store its output'''

    func_name = get_func_name(function)

    return slugify(func_name + '::' + str(arguments))

def get_timestamp():
    '''gets current time'''
    return int(time.time()*1000000000)

def get_cachefile_name(function, arguments, timestamp):
    '''gets the name of the file that will hold the output of function(arguments)
    run at time timestamp'''
    return get_cache_key(function, arguments) \
        + '::' + str(timestamp)+'.cache'

def get_lockstring(scope, cache_root):
    '''gets the name of the index lock'''
    return os.path.join(cache_root, scope, INDEX_NAME)

def index_locked_by_us(scope, cache_root):
    '''returns true if this process holds the lock on the cache index.'''
    return pymutex.locked_by_us(get_lockstring(scope, cache_root))

def unlock_index(scope, cache_root):
    '''release the lock on the cache index.'''
    pymutex.unlock(get_lockstring(scope, cache_root))

def lock_index(scope, cache_root):
    '''acquire the lock on the cache index.
    Returns when the lock has been acquired.
    '''
    pymutex.lock(get_lockstring(scope, cache_root))

def empty_index():
    '''defines what an empty cache index looks like.'''
    return {'cachelist': {}}

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
        index = empty_index()
        write_index(index, scope, cache_root)
    return index

def write_index(index, scope, cache_root):
    '''saves current copy of cache index in memory to disk.

    Must hold the index lock to do this.'''

    assert index_locked_by_us(scope, cache_root)
    indexfile = open(os.path.join(cache_root+scope, INDEX_NAME), 'w')
    pickle.dump(index, indexfile)
    indexfile.close()

def check_cache(function, arguments, scope, cache_root):
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

def get_logfiles(function, arguments, filter_func=lambda x: x, scope=None, cache_root=None):
    '''finds all cache entries tagged as "logs" for function(arguments)'''

    if cache_root is None:
        cache_root = DEFAULT_CACHE_ROOT
    if scope is None:
        scope = DEFAULT_SCOPE
    lock_index(scope, cache_root)
    filtered_logfiles = filter_func(check_cache(function, arguments, scope, cache_root)['logfiles'])
    unlock_index(scope, cache_root)
    return filtered_logfiles

def get_logged_calls(function, scope=None, cache_root=None):
    '''returns a list of dicts with keys {arguments, metadata, timestamp}
    corresponding to all calls of function stored in the cache'''
    if cache_root is None:
        cache_root = DEFAULT_CACHE_ROOT
    if scope is None:
        scope = DEFAULT_SCOPE

    lock_index(scope, cache_root)

    index = load_index(scope, cache_root)
    func_name = get_func_name(function)
    if func_name not in index['cachelist']:
        logged_calls = []
    else:
        logged_calls = index['cachelist'][func_name]
    unlock_index(scope, cache_root)

    return logged_calls

def blank_index_entry():
    '''generates an empty cache index entry to be filled in'''
    return {'cache_file': None, 'cacheTime': 0, 'logfiles': []}

def add_to_index(function, arguments, metadata, timestamp, index, cache_file, setcache_flag):
    '''
    adds an entry corresponding to cache_key with timestamp and metadata to an index
    dictionary.
    setcache_flag is a true/false flag indicated whether the new entry is only a
    write-only log or can be accessed as a cache-hit on a lookup.
    CAREFUL: THIS FUNCTION MODIFIES THE SUPPLIED INDEX DICTIONARY
    '''

    func_name = get_func_name(function)
    cache_key = get_cache_key(function, arguments)
    if cache_key not in index:
        index[cache_key] = blank_index_entry()

    logfile_data = {'file_name': cache_file, 'timestamp': timestamp, \
        'metadata': metadata, 'arguments': arguments, 'function': func_name}
    if is_committed():
        logfile_data['git_hash'] = get_git_hash()

    index[cache_key]['logfiles'].append(logfile_data)
    if setcache_flag and index[cache_key]['cacheTime'] < timestamp:
        index[cache_key]['cache_file'] = cache_file
        index[cache_key]['cacheTime'] = timestamp

    if func_name not in index['cachelist']:
        index['cachelist'][func_name] = []

    index['cachelist'][func_name].append({'arguments': arguments, 'metadata': metadata, \
        'timestamp': timestamp})

def write_entry_to_index(function, arguments, metadata, timestamp, cache_file, setcache_flag, \
        scope, cache_root):
    '''updates the cache index to include a newly-added cached function result'''
    lock_index(scope, cache_root)
    index = load_index(scope, cache_root)
    add_to_index(function, arguments, metadata, timestamp, index, cache_file, setcache_flag)
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
            function = cache_data['function']
            arguments = cache_data['arguments']
            cache_key = cache_data['cache_key']
            assert cache_key == get_cache_key(function, arguments)
            metadata = cache_data['metadata']
            is_cache_hit = cache_data['is_cache_hit']
            timestamp = cache_data['timestamp']
            add_to_index(function, arguments, metadata, timestamp, index, file_name, is_cache_hit)
        except:
            pass
    write_index(index, scope, cache_root)

def get_results_from_cache_file(cache_file, scope=None, cache_root=None):
    '''extracts function output from cached data'''
    if cache_root is None:
        cache_root = DEFAULT_CACHE_ROOT
    if scope is None:
        scope = DEFAULT_SCOPE
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

def cache_function(function, arguments, metadata=None, scope=None, cache_root=None):
    '''
    caches the results of running a function with keyword arguments specified
    by the dictionary arguments in a given scope from the cache_root.
    Note that you should be very careful about caching functions that have
    side-effects as recovering the function results from cache will not
    re-execute side effects.
    '''
    if cache_root is None:
        cache_root = DEFAULT_CACHE_ROOT
    if scope is None:
        scope = DEFAULT_SCOPE

    touch_path(scope, cache_root)

    cache_file = get_cache_file(function, arguments, scope, cache_root)
    if cache_file != None:
        return get_results_from_cache_file(cache_file, scope, cache_root)

    return log_function(function, arguments, metadata, True, scope, cache_root)

def is_pickleable(test_object):
    '''
    detects if an object is pickleable and returns true if so.
    '''
    flag = True
    try:
        pickle.dumps(test_object)
    except pickle.PicklingError:
        flag = False
    return flag

def process_arguments(arguments):
    '''
    converts arguments to repr strings if they are unpickleable
    '''
    return {arg: arguments[arg] \
        if is_pickleable(arguments[arg]) else arguments[arg].__repr__() for arg in arguments}


def log_function(function, arguments, metadata=None, use_as_cache=True, scope=None, cache_root=None):
    '''
    runs the function on the arguments and stores the restult in a logfile.
    These results can be recalled as a cached result of the function later if
    use_as_cache = True.
    Notice that this ALWAYS runs the function, regardless of whether it has been
    cached already.

    metadata is an object that is stored in the metadata section of the cache index.
    '''
    if cache_root is None:
        cache_root = DEFAULT_CACHE_ROOT
    if scope is None:
        scope = DEFAULT_SCOPE

    touch_path(scope, cache_root)

    unprocessed_args = arguments
    arguments = process_arguments(arguments)

    cache_key = get_cache_key(function, arguments)
    cache_data = {}
    cache_data['cache_key'] = cache_key
    cache_data['is_cache_hit'] = use_as_cache
    cache_data['function'] = function.__name__
    cache_data['arguments'] = arguments
    cache_data['results'] = function(**unprocessed_args)
    timestamp = get_timestamp()
    cache_data['timestamp'] = timestamp
    cache_data['metadata'] = metadata
    cache_data['cachelogversion'] = VERSION

    cache_file = get_cachefile_name(function, arguments, timestamp)

    write_data_to_cache_file(cache_data, cache_file, scope, cache_root)
    write_entry_to_index(function, arguments, metadata, timestamp, cache_file, use_as_cache, \
        scope, cache_root)
    return cache_data['results']

def cachify(function, scope=None, cache_root=None):
    '''returns a wrapped version of a supplied function
    that will check for and return a cached result when called
    and store results in the cache if no cached result is available.'''
    if cache_root is None:
        cache_root = DEFAULT_CACHE_ROOT
    if scope is None:
        scope = DEFAULT_SCOPE

    args_list = inspect.getargspec(function).args
    def cachified_function(*args, **kwargs):
        '''cachified version of a function'''
        args_dict = dict(zip(args_list, args))
        args_dict.update(kwargs)
        return cache_function(function, args_dict, scope=scope, cache_root=cache_root)
    if function.__doc__:
        cachified_function.__doc__ = function.__doc__ + '\n**** cachified ****'
    cachified_function.__name__ = function.__name__

    return cachified_function

def logify(function, use_as_cache=True, scope=None, cache_root=None):
    '''returns a wrapped version of a supplied function
    that will ALWAYS run the function and store the result
    in the cache with the "log" tag.'''

    if cache_root is None:
        cache_root = DEFAULT_CACHE_ROOT
    if scope is None:
        scope = DEFAULT_SCOPE
    args_list = inspect.getargspec(function).args
    def logified_function(*args, **kwargs):
        '''logified vesion of a function'''
        args_dict = dict(zip(args_list, args))
        args_dict.update(kwargs)
        return log_function(function, args_dict, use_as_cache=use_as_cache, scope=scope, \
            cache_root=cache_root)

    if function.__doc__:
        logified_function.__doc__ = function.__doc__ + '\n**** logified ****'
    logified_function.__name__ = function.__name__

    return logified_function

def get_save_func(data):
    '''returns a function that can be used with the cache machinery
    to save some arbitrary data'''
    def save_data(title):
        '''returns pre-defined data under a  supplied title'''
        return {'data': data, 'title': title}
    return save_data

def save(data, title, metadata=None, scope=None, cache_root=None):
    '''store some given data in the cache with a given title and metadata.
    This function timestamps the data, so save can be called many times with
    identical arguments without overwriting old data.'''

    if cache_root is None:
        cache_root = DEFAULT_CACHE_ROOT
    if scope is None:
        scope = DEFAULT_SCOPE
    save_func = get_save_func(data)
    arguments = {'title': title}
    log_function(save_func, arguments, metadata, False, scope, cache_root)

def get(title, filter_func=lambda x: x, scope=None, cache_root=None):
    '''finds all data stored under a given title, filtering the results using filter_func'''

    if cache_root is None:
        cache_root = DEFAULT_CACHE_ROOT
    if scope is None:
        scope = DEFAULT_SCOPE
    save_func = get_save_func(None)
    arguments = {'title': title}

    logfiles = get_logfiles(save_func, arguments, filter_func, scope, cache_root)

    return [dict(logfile.items() + \
        [('results', get_results_from_cache_file(logfile['file_name'], scope, cache_root))]) \
    for logfile in logfiles]

def get_last(title, filter_func=lambda x: x, scope=None, cache_root=None):
    '''
    returns the most recent saved data under the given title that
    passes the supplied filter.'''

    filtered_results = get(title, filter_func, scope=scope, cache_root=cache_root)
    last_timestamp = 0
    saved_data = None
    for result in filtered_results:
        if result['timestamp'] > last_timestamp:
            last_timestamp = result['timestamp']
            saved_data = result['results']['data']
    return saved_data
