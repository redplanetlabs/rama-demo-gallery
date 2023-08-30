# rama-demo-gallery

This repository contains a collection of short, self-contained, thoroughly commented examples of applying [Rama](https://redplanetlabs.com) towards a variety of use cases. All of these examples can scale to millions of reads and writes per second. Without Rama, these examples would require using and operating multiple pieces of specialized infrastructure. The examples are:

- [ProfileModule](src/main/java/rama/gallery/profiles/ProfileModule.java): Implements account registration, profiles, and profile editing. See [ProfileModuleTest](src/test/java/rama/gallery/ProfileModuleTest.java) for how to interact with this module to register accounts, edit profiles, and query for profile data.
- [TimeSeriesModule](src/main/java/rama/gallery/timeseries/TimeSeriesModule.java): Implements time-series analytics for latencies of rendering URLs on a website. Aggregates an index of min/max/average statistics for minute, hour, day, and monthly granularities. See [TimeSeriesModuleTest](src/test/java/rama/gallery/TimeSeriesModuleTest.java) for how clients perform various kinds of range queries on this index.
- [TopUsersModule](src/main/java/rama/gallery/topusers/TopUsersModule.java): Implements top-N analytics on user activity in the context of an e-commerce site. It tracks in realtime the top 500 users who have spent the most money on the service. See [TopUsersModuleTest](src/test/java/rama/gallery/TopUsersModuleTest.java) for examples of querying the module.

## Running tests

Tests can be run with `mvn test`, or a specific test class can be run like `mvn test -Dtest=ProfileModuleTest`.
