# m2m : MongoDB to MySQL replication

m2m is a simple cli tool for replicating MongoDB to MySQL in realtime.

- Enable SQL query on data in NoSQL database
- Enable to be accessed by Excel / Access

## Installation

```bash
$ npm install -g m2m
```

or install it within the project locally:

```bash
$ npm install --save m2m
```

## Preparation

### MongoDB

m2m uses [Replica Set](http://docs.mongodb.org/manual/replication/) feature in MongoDB. But you don't have to replicate between MongoDB actually. Just follow the step below.

Start a new mongo instance with no data:

```bash
$ mongod --replSet "rs0" --oplogSize 100
```

Open another terminal, and go to MongoDB Shell:

```bash
$ mongo
....
> rs.initiate()
```

`rs.initiate()` command prepare the collections that is needed for replication.

### MySQL

Launch MySQL instance, and create the new database to use. The tables will be created or updated when syncing. You'll see `mongo_to_mysql`, too. This is needed to store the information for syncing. (don't remove it)

### Configuration

Create a new `m2mfile.json` file like this:

```json
{
  "service": "mycol001",
  "mongodb": {
    "host": "127.0.0.1",
    "port": 27017,
    "db": "test",
    "collection": "blog_posts"
  },
  "mysql": {
    "host": "localhost",
    "user": "root",
    "password": "",
    "db": "test",
    "table": "blog_posts"
  },
  "sync_fields": {
    "_id": "int",
    "field1": "int",
    "field2": "int",
    "field3": "string",
    "field4": "string"
  },
  "transform" : {
    "field_name" : {
      "order": "_order"
    },
    "field_value" : {
      "cid": "transform_cid",
    }
  }
}
```

## Usage

At the first run, we need to import all the data from MongoDB:

```bash
$ m2m --config m2mfile.js --import
```

Then start the daemon to streaming data:

```bash
$ m2m --config m2mfile.js
```

or

```bash
$ forever m2m --config m2mfile.js
```

## License

MIT

This library was originally made by @doubaokun as [MongoDB-to-MySQL](https://github.com/doubaokun/MongoDB-to-MySQL) and rewritten by @cognitom.
