##MongoDB to MySQL Data Streaming

Streaming data in MongoDB to MySQL database in realtime. Enable SQL query on
data in NoSQL database.

## Configurations:

1. Update the mongodb configuration in `config.json`

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
  "sync_fields": { // fields store in MySQL
    "_id": "int",  // required field
    "field1": "int",
    "field2": "int",
    "field3": "string",
    "field4": "string"
  },
  "transform" : {
    "field_name" : { // field name changes
      "order": "_order"
    },
    "field_value" : {
      "cid": "transform_cid", // field value changes, need function
    }
  }
}
```

2. Add index manually, for example:

    ALTER TABLE blog_posts ADD PRIMARY KEY (_id);

    ALTER TABLE blog_posts ADD INDEX field1 (field1);

    ALTER TABLE blog_posts ADD INDEX _order_id (_order, _id);

3. Import the old data in MongoDB collection to MySQL table:

    node app.js import

4. Start the daemon to streaming data

    node start app.js or forever start app.js

5. A MySQL table mongo_to_mysql will be created to store required information.

6. Update the transform() to change field names or modify values during streaming.
