# pycache
tool for saving expensive computations on disk or logging repeated computations.
Requires pytest for testing.

```
import pycache

@pycache.cachify
def expensive_function(arg, kwarg=0):
    ...
    return result
    

result = expensive_function(5) #takes a while
result2 = expensive_function(5) #is quick
```

You can also cache functions you didn't define (or don't want to decorate for some reason):

```
result = cache_function(expensive_function, {'arg': 5, 'kwarg': 0})

#or alternatively:
cachified_func = pycache.cachify(expensive_external_function)
result = cachified_func(5)
```

Use the `log` functionality to save function results but always run the function:
```
@pycache.logify
def find_nicest_weather(country):
    weather = get_weather_today(country)
    # Do something with the weather
    return some_place

find_nicest_weather('USA') # = 'Chicago'
find_nicest_weather('India') # = 'Chennai'

#on a different day:
find_nicest_weather('USA') # = 'Cincinatti'
```
There are functions `log_function` and `logify` analgous to `cache_function` and `cachify`.
You can recover a list of the logged function calls with `get_logged_calls`
```
logs = pycache.get_logged_calls(find_nicest_weather)
len(logs) = 3
logs[0].keys # ['timestamp', 'metadata', 'file_name', 'git_hash', 'arguments', 'function']
```

A log dict contains the following key/value pairs:
```
timestamp: an integer identifying when the log was created.
metadata: user-supplied object stored with the log. Provide this as a third argument to log_function or cache_function.
  It is None if not specified.
file_name: name of file on disk that stores the result of the function.
git_hash: git hash of repo when log was generated (not present if there were unstaged changes).
arguments: a dict containing the arguments to the function
function: the name of the function
```
The actual results of the function evaluation are stored in a seperate file to make it easy to look through long lists of calls that return large results. The contents of `logs[0]['file_name']` is a dict containing:
```
results: the actual returned value of the function call
function
arguments
timestamp
metadata
```

The results can be recovered with `get_results_from_cache_file(logs[0]['file_name'])`.

pycache can also be used as a generic way to save data rather than function evaluations:
```
pycache.save(some_data, title, metadata=None)
pycache.get(title) # a list of dicts corresponding to each call of save with the given title. 
  The actual results are found in list_item['saved_data']['results']
pycache.get_last(title) # the result of the last save call with this title (not a dict)
```

The functions `get`, and `get_last` also take an optional argument `filter_func`. `filter_func` should take a list of `log` dicts, and return a list of `log` dicts. `get` and `get_last` will then fetch the results from this new list of `log`s.

Finally, there's a simple utility function `pycache.force_git_commit()` that will throw an error if your code hasn't been committed.
