__version__ = "0.1.0"

import threading
import os
import gevent
import time
import functools

class ConnectionError(Exception): pass
class SocketError(Exception): pass

class State(object):
    def __init__(self, *args, **kwargs):
        self._args = args
        self._kwargs = kwargs
        self._hash = hash((args, tuple(sorted(kwargs.items()))))

    def __hash__(self):
        return self._hash

    def __eq__(self, other):
        return self._hash == other._hash

class StateSet(set):
    pass

class NotImplementedConnection(object):
    def __init__(self, *args, **kwargs):
        raise NotImplementedError

class Connection(object):
    CONNECTING = 0
    CONNECTED = 1
    IN_USE = 2
    BROKEN = 3
    CLOSED = 4
    RECONNECTING = 5

    def __init__(self, conn):
        self._connection = conn
        self.pid = os.getpid()
        conn.wrapper(self)
        self._states = None
        self.state = self.CONNECTED

    def __getattr__(self, name):
        if hasattr(self._connection, name):
            return getattr(self._connection, name)
        raise AttributeError(repr(self._connection) + " don't has attr with name: " + name)

    def reconnect(self):
        if hasattr(self._connection, "reconnect"):
            self.state = self.RECONNECTING
            self._connection.reconnect()
            self._states = conn.default_states()
            self.state = self.CONNECTED
        raise NotImplementedError

    def disconnect(self):
        if hasattr(self._connection, "disconnect"):
            self._connection.disconnect()

class WrappedConnection(object):
    def wrapper(self, conn):
        self._wrapper = conn

class Client(object):
    def __init__(self, max_connections=None, max_idle_connections=0):
        max_connections = max_connections or 2 ** 31
        if not isinstance(max_connections, (int, long)) or max_connections < 0:
            raise ValueError('"max_connections" must be a positive integer')

        if not isinstance(max_idle_connections, (int, long)) or max_idle_connections < 0:
            raise ValueError('"max_idle_connections" must be a positive integer')

        self.max_connections = max_connections
        self.max_idle_connections = max_idle_connections
        self._wrapper = None
        self.reset()

    def __enter__():
        return self

    def __exit__():
        self.close()

    def get_connection(self):
        self._checkpid()
        try:
            connection = self._available_connections.pop()
        except IndexError:
            connection = Connection(self.make_connection())
            connection._states = self._default_states()
        try:
            self._sync(connection)
        except:
            connection.disconnect()
            raise
        self._in_use_connections.add(connection)
        return connection

    def release(self, connection):
        self._checkpid()
        if connection.pid != self.pid:
            return
        self._in_use_connections.remove(connection)
        if len(self._available_connections) >= self.max_idle_connections:
            connection.close()
            connection.state = Connection.CLOSED
            self._created_connections -= 1
        elif connection.state == Connection.BROKEN:
            connection.close()
            connection.state = Connection.CLOSED
            self._created_connections -= 1
        else:
            self._available_connections.append(connection)
            connection.state = Connection.CONNECTED

    def close(self):
        for x in self._available_connections:
            x.disconnect()
        for x in self._in_use_connections:
            x.disconnect()

    def reset(self):
        self.pid = os.getpid()
        self._created_connections = 0
        self._available_connections = []
        self._in_use_connections = set()
        self._check_lock = threading.Lock()
        self._states = {}
        self._setter = {}
        self._unsetter = {}

    def make_connection(self, **kwargs):
        if self._created_connections >= self.max_connections:
            raise ConnectionError("Too many connections")
        conn = self.ex(self._connecter)
        self._created_connections += 1
        return conn

    def _checkpid(self):
        if self.pid != os.getpid():
            with self._check_lock:
                if self.pid == os.getpid():
                    # another thread already did the work while we waited
                    # on the lock.
                    return
                # self.disconnect()
                self.reset()

    def connect(self):
        return Context(self)

    def connecting(self, retry=0):
        return Context(self, waiting=True, retry=retry)

    def connecter(self, **kwargs):
        def _fn(fn):
            self._connecter = fn
            return self.__not_callable(fn)
        return _fn

    def default_states(self, **kwargs):
        def _fn(fn):
            self._default_states = fn
            self._states = fn()
            return self.__not_callable(fn)
        return _fn


    def state(self, **kwargs):
        def _fn(fn):
            state_key = kwargs.pop("key", None) or fn.__name__
            self._setter[state_key] = fn
            assert isinstance(state_key, str), \
                   "%r is not a str" % state_key
            def __fn(wrapped, *args, **kwargs):
                self._states[state_key] = State(*args, **kwargs)
                wrapped._wrapper._states[state_key] = State(*args, **kwargs)
                return fn(wrapped, *args, **kwargs)
            return __fn
        return _fn

    def attend(self, **kwargs):
        def _fn(fn):
            state_key = kwargs.pop("key", None) or fn.__name__
            assert isinstance(state_key, str), \
                   "%r is not a str" % state_key
            self._setter[state_key] = fn
            def __fn(wrapped, *args, **kwargs):
                if state_key not in self._states:
                    self._states[state_key] = StateSte()
                self._states[state_key].add(State(*args, **kwargs))
                if state_key not in wrapped._wrapper._states:
                    wrapped._wrapper._states[state_key] = StateSet()
                wrapped._wrapper._states[state_key].add(State(*args, **kwargs))
                return fn(wrapped, *args, **kwargs)
            return __fn
        return _fn

    def ignore(self, **kwargs):
        def _fn(fn):
            state_key = kwargs.pop("key", None) or fn.__name__
            assert isinstance(state_key, str), \
                   "%r is not a str" % state_key
            self._unsetter[state_key] = fn
            def __fn(wrapped, *args, **kwargs):
                if state_key in self._states:
                    self._states[state_key].discard(State(*args, **kwargs))
                if state_key in wrapped._wrapper._states:
                    wrapped._wrapper._states[state_key].discard(State(*args, **kwargs))
                return fn(wrapped, *args, **kwargs)
            return __fn
        return _fn

    def wrapper(self, **kwargs):
        def _fn(fn):
            self._wrapper = fn
            return self.__not_callable(fn)
        return _fn

    def ex(self, fn, *args, **kwargs):
        if self._wrapper is None:
            return fn(*args, **kwargs)
        return self._wrapper(fn, *args, **kwargs)

    def retry(self, **kwargs):
        max_retry = kwargs.pop("retry", 0)
        if not isinstance(max_retry, (int, long)) or max_retry < 0:
            raise ValueError('"retry" must be a positive integer')
        origin_name = kwargs.pop("self", None)
        _name = origin_name or "self"
        _xrange = max_retry + 1 if max_retry > 0 else 2 ** 31
        def _fn(fn):
            self_name = _name
            self_idx = -1
            varnames = fn.func_code.co_varnames[:fn.func_code.co_argcount]
            if isinstance(self_name, str):
                if self_name in varnames:
                    self_idx = varnames.index(self_name)
                """
                    count catch connection from kwargs
                # else:
                #    raise AttributeError(repr(fn) + " has no argument with '{self_name}'")
                """
            elif isinstance(self_name, "int"):
                if len(varnames) > self_name:
                    self_idx = self_name
                    self_name = varnames[self_idx]
                else:
                    raise AttributeError(repr(fn) + " has no more arguments")

            @functools.wraps(fn)
            def __fn(*args, **kwargs):
                __xrange = xrange(_xrange)
                for trying in __xrange:
                    try:
                        # print trying, time.time(), fn.__name__
                        if trying > 0:
                            connection = None
                            if self_idx >= 0 and len(args) > self_idx:
                                connection = args[self_idx]
                            elif isinstance(self_name, str):
                                if self_name in kwargs:
                                    connection = kwargs[self_name]
                                elif origin_name is not None:
                                    raise KeyError("can't found % in kwargs" % self_name)
                            else:
                                connection = self_name
                            if connection is not None and hasattr(connection, "reconnect"):
                                self.ex(connection.reconnect)
                                connection._wrapper._states = self._default_states()
                            __xrange = xrange(_xrange)
                        return self.ex(fn, *args, **kwargs)
                    except SocketError, e:
                        if trying > 0:
                            gevent.sleep(1<<min(2, trying - 1)) # max sleep 4 seconds
                        if max_retry > 0 and trying >= max_retry:
                            raise e

            return __fn
        return _fn

    def _sync(self, conn):
        if self._states == conn._states:
            return
        for st in self._states:
            state = self._states[st]
            if st in conn._states and state == conn._states[st]:
                continue
            wrapped_connection = conn._connection
            if isinstance(state, StateSet):
                conn_state = conn._states[st]
                plus = state - conn_state
                minus = conn_state - state
                for s in plus:
                    self._setter[st](wrapped_connection, *s._args, **s._kwargs)
                for s in minus:
                    self._unsetter[st](wrapped_connection, *s._args, **s._kwargs)
                conn._states[st] |= plus
                conn._states[st] -= minus
            else:
                self._setter[st](wrapped_connection, *state._args, **state._kwargs)
                conn._states[st] = state

    @staticmethod
    def __not_callable(fn):
        @functools.wraps(fn)
        def _fn(*args, **kwargs):
            msg = str(fn) + " can't be called any more"
            raise TypeError(msg)
        return _fn

class Context(object):
    def __init__(self, connections, waiting=False, retry=0):
        self._connections = connections
        self._connection = None
        self._waiting = waiting
        self._retry = retry

    def __enter__(self):
        if self._waiting:
            @self._connections.retry(retry=self._retry)
            def get_connection():
                return self._connections.get_connection()
            self._connection = get_connection()
        else:
            self._connection = self._connections.get_connection()
        self._connection.state = Connection.IN_USE
        return self._connection

    def __exit__(self, exc_type, exc_value, exc_tb):
        if exc_tb is not None:
            self._connection.state = Connection.BROKEN
        self._connections.release(self._connection)
        if exc_tb is not None:
            return False
        return True

if __name__ == "__main__":
    from main import main
    main()

