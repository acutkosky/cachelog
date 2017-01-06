"""
Tests for cachelog
"""

import pytest
import cachelog

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
    cachelog.set_cache_root(str(tmpdir))
    assert cachelog.DEFAULT_CACHE_ROOT == str(tmpdir)

def test_caching():
    args = {'x': 1, 'y': 2, 'kw': 3}
    initial_canary = SIDE_EFFECT_CANARY

    #Run function once
    result = cachelog.cache_function(func_to_cache, args)
    assert result == 6
    assert SIDE_EFFECT_CANARY == initial_canary+1

    #Function should not be re-run - results should come from cache
    result = cachelog.cache_function(func_to_cache, args)
    assert result == 6
    assert SIDE_EFFECT_CANARY == initial_canary+1

    new_args = {'x': 1, 'y': 1}
    #Function should be re-run
    result = cachelog.cache_function(func_to_cache, new_args)
    assert result == 2
    assert SIDE_EFFECT_CANARY == initial_canary+2

    #Old results should still be in cache
    result = cachelog.cache_function(func_to_cache, args)
    assert result == 6
    assert SIDE_EFFECT_CANARY == initial_canary+2

    #New results should also be in cache
    result = cachelog.cache_function(func_to_cache, new_args)
    assert result == 2
    assert SIDE_EFFECT_CANARY == initial_canary+2

def test_logging():
    args = {'x': 1, 'kw': 4}
    initial_canary = SIDE_EFFECT_CANARY

    initial_logs = cachelog.get_logfiles(func_to_log, args)
    assert len(initial_logs) == 0

    result = cachelog.log_function(func_to_log, args)
    assert result == 5
    assert SIDE_EFFECT_CANARY == initial_canary+1
    intermediate_logs = cachelog.get_logfiles(func_to_log, args)
    assert len(intermediate_logs) == 1

    new_args = {'x': 3}
    result = cachelog.log_function(func_to_log, new_args)
    assert result == 3
    assert SIDE_EFFECT_CANARY == initial_canary+2
    intermediate_logs = cachelog.get_logfiles(func_to_log, new_args)
    assert len(intermediate_logs) == 1

    result = cachelog.log_function(func_to_log, args)
    assert result == 5
    assert SIDE_EFFECT_CANARY == initial_canary+3
    final_logs = cachelog.get_logfiles(func_to_log, args)
    assert len(final_logs) == 2

def test_cachify():
    initial_canary = SIDE_EFFECT_CANARY
    cachified_func = cachelog.cachify(func_to_cache)

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
    logified_func = cachelog.logify(func_to_log)

    initial_log_length = len(cachelog.get_logged_calls(logified_func))
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


    final_log_length = len(cachelog.get_logged_calls(logified_func))
    assert final_log_length == initial_log_length + 3

def test_save_data():
    data = ['random', 'data', 'here', 1337]
    title = 'Title'
    new_title = 'New Title'
    cachelog.save(data, title)
    cachelog.save(data, new_title)
    recovered = cachelog.get_last(title)
    assert recovered == data

    new_data = ['new', 'data']
    cachelog.save(new_data, new_title)
    recovered = cachelog.get_last(new_title)
    assert recovered == new_data

    recovered = cachelog.get_last(title)
    assert recovered == data

def test_process_func_calls():
    def func_to_process(x):
        return x*2

    logified_func = cachelog.logify(func_to_process)
    for x in xrange(10):
        logified_func(x)

    def processor(y):
        return y/2

    processed_calls = cachelog.process_logged_function_calls(func_to_process, processor)

    for a, b in zip(xrange(10), processed_calls):
        assert a == b


