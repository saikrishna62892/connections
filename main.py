from connections import Client, SocketError as ClientSocketError, WrappedConnection, State as ClientState
from beanstalkc import Connection as BeanstalkConnection, SocketError as BeanstalkSocketError

def BeanstalkClient(host = "127.0.0.1", port = 11300,
            max_connections=5, max_idle_connections=0):
    client = Client(max_connections=max_connections,
            max_idle_connections=max_idle_connections)
    class Connection(BeanstalkConnection, WrappedConnection):

        @client.retry(retry=3)
        def put(self, *args, **kwargs):
            return super(Connection, self).put(*args, **kwargs)

        @client.retry()
        def reserve(self):
            return super(Connection, self).reserve()

        @client.state(key="using")
        def use(self, name):
            return super(Connection, self).use(name)

        @client.attend(key="watching")
        def watch(self, name):
            return super(Connection, self).watch(name)

        @client.ignore(key="watching")
        def ignore(self, name):
            return super(Connection, self).ignore(name)

        def disconnect(self):
            return super(Connection, self).close()

        @staticmethod
        @client.default_states()
        def default_states():
            return {
                       "using": ClientState("default"),
                       "watching":set([
                           ClientState("default")
                           ])
                   }

    @client.connecter()
    def connect():
        return Connection(host=host, port=port)

    @client.wrapper()
    def wrap(fn, *args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except BeanstalkSocketError, e:
            raise ClientSocketError(e)

    return client

def main():
    client = BeanstalkClient()
    with client.connecting() as connection:
        connection.use("tsanyen")

    with client.connecting() as connection:
        connection.put("tsanyen")
        print connection.using()

    client.close()

if __name__ == "__main__":
    main()
