# Development

Enter the Docker container for development:

```bash
$ bash dev/up
```

To install dependencies:

```bash
$ npm install
```

To run the tests:

```bash
$ npm test
```

To test the code manually, start `momy`:

```bash
$ npm run try
```

Then, open a new terminal on the host (not inside the container), and run mongo client:

```bash
$ bash dev/mongo
```

Do something, and run mysql client to check syncing:

```bash
$ bash dev/mysql
```
