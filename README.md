# Minion Agent Client

This is the Crystal version of the Minion Agent Client.

It provides a Crystal Language library to connect to and interact with a Minion Streamserver.

Key features currently:

* Connects to a streamserver
* Basic authentication
* Can send logs
* Sophisticated handling of server failures; messages and logs are queued locally, and spooled back to the server when it reconnects.
* Is generally robust to poor network connections and losing connectivity.
* Interactive Agent REPL Shell to manually send and receive from the stream server.

TODO:

* Fix failure handling. It actually has a bug right now.
* Add support for TLS encryption of the socket communications.
* Add a real debug option to show all communications.

## Communications Protocol

The payloads are serialized using the MessagePack protocol.  MessagePack does
not serialize the length of the serialized data, which negatively affects
network read performance. To work around this, all data which is sent or
received is prefixed with two bytes which encode the length of the data packet
that follows.

The total size of this two byte header plus the data packet can be no longer
than 8k (8192 bytes).

The two byte length is transfered in Big Endian order.

So, for example, consider the following message:

```
2020-06-10 16:23:50 -06:00|stderr|warn|this is very very serious
```

That is 66 bytes when serialized to MessagePack. Expressed as two bytes, big
endian, that is:

```
00.42
```

Those two bytes should be the first two sent, and then immediately after those
bytes should come the other 66 bytes.
