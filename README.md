##PungweDB

For lack of a better name, this is an attempt at writing a pure java document store, using very little in the way of frameworks, in order to keep things as simple as possible.

The aim is to create a fully functional, append only datastore; that is comparable in features to that of couchbase and mongodb.

###Format
The database itself presently supports it's own form of binary json that supports a range of different data types. This might be changed in the near future to something smaller like kryo (which is a form of java serialisation).

###Status
The database has can store documents directly with full serialisation support as well as a basic BTree (not B+Tree) using memory mapped files.

There is currently no journal and the database is by no means durable.

####Map DB
I have considered using this as a storage engine for the database, but I fear that it's not quite there yet in terms of functionality and although it's appears to be quick; it's not quite there yet for me.

####RocksDB
I have considered porting this to java first, then moving back on to this database.

###Components
####Common
Common functionality shared across all the different components
####Core
Core database.
####Server
Provides admin functionality, cluster management, as well as a full suite of rest services (including web sockets and direct socket support).
####Shell
Command line shell for the database
####Embedded
Embedded version of the database in java (I will look at porting this across to android and iOS at a later date).
####Driver
Java language driver, used as a reference implementation for other drivers.

###Help
If any decent UK java developers come across this and fancy helping with development on this database; drop me a line!
