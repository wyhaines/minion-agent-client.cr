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
