# node-postgres

[![Build Status](https://secure.travis-ci.org/brianc/node-postgres.svg?branch=master)](http://travis-ci.org/brianc/node-postgres)
<span class="badge-npmversion"><a href="https://npmjs.org/package/pg" title="View this project on NPM"><img src="https://img.shields.io/npm/v/pg.svg" alt="NPM version" /></a></span>
<span class="badge-npmdownloads"><a href="https://npmjs.org/package/pg" title="View this project on NPM"><img src="https://img.shields.io/npm/dm/pg.svg" alt="NPM downloads" /></a></span>

This is a fork of [node-postgres](https://github.com/brianc/node-postgres) which includes the following additional features:

# Connection load balancing

Users can use this feature in two configurations.

### Cluster-aware / Uniform connection load balancing

In the cluster-aware connection load balancing, connections are distributed across all the tservers in the cluster, irrespective of their placements.

To enable the cluster-aware connection load balancing, provide the parameter `loadBalance` set to true as `loadBalance=true` in the connection url or the connection string (DSN style).

```
"postgresql://username:password@localhost:5433/database_name?loadBalance=true"
```

With this parameter specified in the url, the driver will fetch and maintain the list of tservers from the given endpoint (`localhost` in above example) available in the YugabyteDB cluster and distribute the connections equally across them.

This list is refreshed every 5 minutes, when a new connection request is received.

Application needs to use the same connection url to create every connection it needs, so that the distribution happens equally.

### Topology-aware connection load balancing

With topology-aware connnection load balancing, users can target tservers in specific zones by specifying these zones as `topologyKeys` with values in the format `cloudname.regionname.zonename`. Multiple zones can be specified as comma separated values.

The connections will be distributed equally with the tservers in these zones.

Note that, you would still need to specify `loadBalance=true` to enable the topology-aware connection load balancing.

```
"postgresql://username:password@localhost:5433/database_name?loadBalance=true&topologyKeys=cloud1.region1.zone1,cloud1.region1.zone2"
```
### Specifying fallback zones

For topology-aware load balancing, you can now specify fallback placements too. This is not applicable for cluster-aware load balancing.
Each placement value can be suffixed with a colon (`:`) followed by a preference value between 1 and 10.
A preference value of `:1` means it is a primary placement. A preference value of `:2` means it is the first fallback placement and so on.If no preference value is provided, it is considered to be a primary placement (equivalent to one with preference value `:1`). Example given below.

```
"postgres://username:password@localhost:5433/database_name?loadBalance=true&topologyKeys=cloud1.region1.zone1:1,cloud1.region1.zone2:2";
```

You can also use `*` for specifying all the zones in a given region as shown below. This is not allowed for cloud or region values.

```
"postgres://username:password@localhost:5433/database_name?loadBalance=true&topologyKeys=cloud1.region1.*:1,cloud1.region2.*:2";
```

The driver attempts to connect to a node in following order: the least loaded node in the 1) primary placement(s), else in the 2) first fallback if specified, else in the 3) second fallback if specified and so on.
If no nodes are available either in primary placement(s) or in any of the fallback placements, then nodes in the rest of the cluster are attempted.
And this repeats for each connection request.

## Specifying Refresh Interval

Users can specify Refresh Time Interval, in seconds. It is the time interval between two attempts to refresh the information about cluster nodes. Default is 300. Valid values are integers between 0 and 600. Value 0 means refresh for each connection request. Any value outside this range is ignored and the default is used.

To specify Refresh Interval, use the parameter `ybServersRefreshInterval` in the connection url or the connection string.

```
"postgres://username:password@localhost:5433/database_name?ybServersRefreshInterval=X&loadBalance=true&topologyKeys=cloud1.region1.*:1,cloud1.region2.*:2";
```
Here, X is the value of the refresh interval (seconds) in integer. 

To know more visit the [docs page](https://docs.yugabyte.com/preview/drivers-orms/).

For a working example which demonstrates the configurations of connection load balancing see the [driver-examples](https://github.com/yugabyte/driver-examples/tree/main/nodejs) repository.

Non-blocking PostgreSQL client for Node.js. Pure JavaScript and optional native libpq bindings.

## Monorepo

This repo is a monorepo which contains the core [pg](https://github.com/brianc/node-postgres/tree/master/packages/pg) module as well as a handful of related modules.

- [pg](https://github.com/brianc/node-postgres/tree/master/packages/pg)
- [pg-pool](https://github.com/brianc/node-postgres/tree/master/packages/pg-pool)
- [pg-cursor](https://github.com/brianc/node-postgres/tree/master/packages/pg-cursor)
- [pg-query-stream](https://github.com/brianc/node-postgres/tree/master/packages/pg-query-stream)
- [pg-connection-string](https://github.com/brianc/node-postgres/tree/master/packages/pg-connection-string)
- [pg-protocol](https://github.com/brianc/node-postgres/tree/master/packages/pg-protocol)

## Documentation

Each package in this repo should have its own readme more focused on how to develop/contribute. For overall documentation on the project and the related modules managed by this repo please see:

### :star: [Documentation](https://node-postgres.com) :star:

The source repo for the documentation is https://github.com/brianc/node-postgres-docs.

### Features

- Pure JavaScript client and native libpq bindings share _the same API_
- Connection pooling
- Extensible JS ↔ PostgreSQL data-type coercion
- Supported PostgreSQL features
  - Parameterized queries
  - Named statements with query plan caching
  - Async notifications with `LISTEN/NOTIFY`
  - Bulk import & export with `COPY TO/COPY FROM`

### Extras

node-postgres is by design pretty light on abstractions. These are some handy modules we've been using over the years to complete the picture.
The entire list can be found on our [wiki](https://github.com/brianc/node-postgres/wiki/Extras).

## Support

node-postgres is free software. If you encounter a bug with the library please open an issue on the [GitHub repo](https://github.com/brianc/node-postgres). If you have questions unanswered by the documentation please open an issue pointing out how the documentation was unclear & I will do my best to make it better!

When you open an issue please provide:

- version of Node
- version of Postgres
- smallest possible snippet of code to reproduce the problem

You can also follow me [@briancarlson](https://twitter.com/briancarlson) if that's your thing. I try to always announce noteworthy changes & developments with node-postgres on Twitter.

## Sponsorship :two_hearts:

node-postgres's continued development has been made possible in part by generous finanical support from [the community](https://github.com/brianc/node-postgres/blob/master/SPONSORS.md) and these featured sponsors:

<div align="center">
  <p>
    <a href="https://crate.io" target="_blank">
      <img height="80" src="https://node-postgres.com/crate-io.png" />
    </a>
  </p>
  <p>
    <a href="https://www.eaze.com" target="_blank">
      <img height="80" src="https://node-postgres.com/eaze.png" />
    </a>
  </p>
</div>

If you or your company are benefiting from node-postgres and would like to help keep the project financially sustainable [please consider supporting](https://github.com/sponsors/brianc) its development.

## Contributing

**:heart: contributions!**

I will **happily** accept your pull request if it:

- **has tests**
- looks reasonable
- does not break backwards compatibility

If your change involves breaking backwards compatibility please please point that out in the pull request & we can discuss & plan when and how to release it and what type of documentation or communication it will require.

### Setting up for local development

1. Clone the repo
2. From your workspace root run `yarn` and then `yarn lerna bootstrap`
3. Ensure you have a PostgreSQL instance running with SSL enabled and an empty database for tests
4. Ensure you have the proper environment variables configured for connecting to the instance
5. Run `yarn test` to run all the tests

## Troubleshooting and FAQ

The causes and solutions to common errors can be found among the [Frequently Asked Questions (FAQ)](https://github.com/brianc/node-postgres/wiki/FAQ)

## License

Copyright (c) 2010-2020 Brian Carlson (brian.m.carlson@gmail.com)

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
