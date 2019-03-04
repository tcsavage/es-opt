# ES Opt

Optimiser for Elasticsearch queries.

## Usage

Requires the Clojure CLI.

You can run the program thus:

```
clj -m main
```

The query is read as JSON from stdin, and the optimised form goes to stdout. Statistics and logs to to stderr.

```
clj -m main <query.json >query.opt.json
```
