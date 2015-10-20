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

m2m uses [Replica Set](http://docs.mongodb.org/manual/replication/) feature in MongoDB. But you don't have to replicate between MongoDB actually. Just follow the steps below.

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
  "src": "mongodb://localhost:27017/dbname",
  "dist": "mysql://root@localhost:3306/dbname",
  "prefix": "t_",
  "collections": {
    "collection1": {
      "_id": "int",
      "field1": "int",
      "field2": "string",
      "field3": "boolean"
    },
    "collection2": {
      "_id": "int",
      "field1": "int",
      "field2": "string",
      "field3": "boolean"
    }
  }
}
```

- `src`: the URL of the MongoDB server
- `dist`: the URL of the MySQL server
- `prefix`: optional prefix for table name. The name of the table would be `t_collection1` in the example above.
- `collections`: set the collections and fields to sync

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
