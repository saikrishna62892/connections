from connections import Client, SocketError as ClientSocketError, WrappedConnection, State as ClientState, StateSet
from beanstalkc import Connection as BeanstalkConnection, SocketError as BeanstalkSocketError
import gevent

def BeanstalkClient(host = "127.0.0.1", port = 11300,
            max_connections=5, max_idle_connections=0):
    client = Client(max_connections=max_connections,
            max_idle_connections=max_idle_connections)
    class Connection(BeanstalkConnection, WrappedConnection):

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

        @client.default_states()
        def __default_states():
            return {
                       "using": ClientState("default"),
                       "watching":StateSet([
                           ClientState("default")
                           ])
                   }

        @client.connecter()
        def __connecter():
            return Connection(host=host, port=port)

        @client.notcallable
        @client.catcher()
        def __catch(fn, *args, **kwargs):
            try:
                return fn(*args, **kwargs)
            except BeanstalkSocketError, e:
                print "catcher:", e # catch all socket err
                raise ClientSocketError(e)

        @client.notcallable
        @client.delayer()
        def __delay(trying):
            if trying > 0:
                gevent.sleep(1<<min(2, trying - 1)) # max sleep 4 seconds

    return client

def main():
    client = BeanstalkClient()

    def catch_connecting_retry(e):
        print e

    with client.connecting(catch=catch_connecting_retry) as connection:
        connection.use("tsanyen")
        connection.watch("tsanyen")

    with client.connecting() as connection:
        connection.put("data")
        def catch_put_retry(e):
            if isinstance(e, ClientSocketError):
                print e
                return True

        retry_put = client.retry(retry=3, catch=catch_put_retry)(connection.put)
        retry_put("data")
        print connection.using()

    with client.connecting() as connection:
        for x in xrange(2):
            job = connection.reserve()
            print job.body
            job.delete()

    client.close()

if __name__ == "__main__":
    main()
