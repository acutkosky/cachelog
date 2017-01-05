"""
Tests for pycache
"""

import pytest
import pycache

SIDE_EFFECT_CANARY = 0
LOG_FUNC_CALLS = 0

def func_to_cache(x, y, kw=0):
    global SIDE_EFFECT_CANARY
    SIDE_EFFECT_CANARY += 1
    return x + y + kw

def func_to_log(x, kw = 0):
    global SIDE_EFFECT_CANARY, LOG_FUNC_CALLS
    SIDE_EFFECT_CANARY += 1
    LOG_FUNC_CALLS += 1

    return x + kw

def test_init(tmpdir):
    pycache.set_cache_root(str(tmpdir))
    assert pycache.DEFAULT_CACHE_ROOT == str(tmpdir)

def test_caching():
    args = {'x': 1, 'y': 2, 'kw': 3}
    initial_canary = SIDE_EFFECT_CANARY

    #Run function once
    result = pycache.cache_function(func_to_cache, args)
    assert result == 6
    assert SIDE_EFFECT_CANARY == initial_canary+1

    #Function should not be re-run - results should come from cache
    result = pycache.cache_function(func_to_cache, args)
    assert result == 6
    assert SIDE_EFFECT_CANARY == initial_canary+1

    new_args = {'x': 1, 'y': 1}
    #Function should be re-run
    result = pycache.cache_function(func_to_cache, new_args)
    assert result == 2
    assert SIDE_EFFECT_CANARY == initial_canary+2

    #Old results should still be in cache
    result = pycache.cache_function(func_to_cache, args)
    assert result == 6
    assert SIDE_EFFECT_CANARY == initial_canary+2

    #New results should also be in cache
    result = pycache.cache_function(func_to_cache, new_args)
    assert result == 2
    assert SIDE_EFFECT_CANARY == initial_canary+2

def test_logging():
    args = {'x': 1, 'kw': 4}
    initial_canary = SIDE_EFFECT_CANARY

    initial_logs = pycache.get_logfiles(func_to_log, args)
    assert len(initial_logs) == 0

    result = pycache.log_function(func_to_log, args)
    assert result == 5
    assert SIDE_EFFECT_CANARY == initial_canary+1
    intermediate_logs = pycache.get_logfiles(func_to_log, args)
    assert len(intermediate_logs) == 1

    new_args = {'x': 3}
    result = pycache.log_function(func_to_log, new_args)
    assert result == 3
    assert SIDE_EFFECT_CANARY == initial_canary+2
    intermediate_logs = pycache.get_logfiles(func_to_log, new_args)
    assert len(intermediate_logs) == 1

    result = pycache.log_function(func_to_log, args)
    assert result == 5
    assert SIDE_EFFECT_CANARY == initial_canary+3
    final_logs = pycache.get_logfiles(func_to_log, args)
    assert len(final_logs) == 2

def test_cachify():
    initial_canary = SIDE_EFFECT_CANARY
    cachified_func = pycache.cachify(func_to_cache)

    result = cachified_func(2, 3, 4)
    assert result == 9
    assert SIDE_EFFECT_CANARY == initial_canary + 1

    result = cachified_func(3, 4)
    assert result == 7
    assert SIDE_EFFECT_CANARY == initial_canary + 2

    result = cachified_func(2, 3, 4)
    assert result == 9

    result = cachified_func(3, 4)
    assert result == 7

    assert SIDE_EFFECT_CANARY == initial_canary + 2

def test_logify():
    initial_canary = SIDE_EFFECT_CANARY
    logified_func = pycache.logify(func_to_log)

    initial_log_length = len(pycache.get_logged_calls(logified_func))
    assert initial_log_length == LOG_FUNC_CALLS

    result = logified_func(2, 3)
    assert result == 5
    assert SIDE_EFFECT_CANARY == initial_canary + 1

    result = logified_func(3)
    assert result == 3
    assert SIDE_EFFECT_CANARY == initial_canary + 2

    result = logified_func(2, 3)
    assert result == 5
    assert SIDE_EFFECT_CANARY == initial_canary + 3


    final_log_length = len(pycache.get_logged_calls(logified_func))
    assert final_log_length == initial_log_length + 3

def test_save_data():
    data = ['random', 'data', 'here', 1337]
    title = 'Title'
    new_title = 'New Title'
    pycache.save(data, title)
    pycache.save(data, new_title)
    recovered = pycache.get_last(title)
    assert recovered == data

    new_data = ['new', 'data']
    pycache.save(new_data, new_title)
    recovered = pycache.get_last(new_title)
    assert recovered == new_data

    recovered = pycache.get_last(title)
    assert recovered == data
