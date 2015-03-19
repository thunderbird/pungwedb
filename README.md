##PungweDB

For lack of a better name, this is an attempt at writing a pure java document store, using very little in the way of frameworks, in order to keep things as simple as possible.

The aim is to create a fully functional, append only datastore; that is comparable in features to that of couchbase and mongodb.

###Format
The database itself presently supports it's own form of binary json that supports a range of different data types. This might be changed in the near future to something smaller like kryo (which is a form of java serialisation).

At the core of the database is a standard java map implementation based on the ConcurrentNavigableMap that stores data in memory or to disk. This map will allow for storage of json style documents or blobs of data (Strings, numbers, binary data, etc).

###Status
The database has can store documents directly with full serialisation support.

It is still a long way off. Core components are still being worked on, the durability work still needs to be done, as well as replication, command processing, etc. At the moment it's not much further along that core maps, queues and unit tests.

##Collections / Maps
- BTreeMap - basic BTree based on ConcurrentNavigableMap
- Queue - basic push / pop queue that uses a variety of storage mediums

There is currently no journal and the database is by no means durable.

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
