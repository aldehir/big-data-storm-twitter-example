# Storm Twitter Example
Reads in tweets from Twitter's public Streaming API, extracts hashtags, and
accumulates counts in a Redis instance.

## Requirements
* [Apache Storm 0.9.3](https://storm.apache.org/)
* [Apache Maven](http://maven.apache.org/)
* [Redis](http://redis.io/)

### Installing Maven
Install through your distribution's package manager.

***Ubuntu***

    $ sudo apt-get install maven

***Fedora***

    $ sudo yum install maven

### Installing Apache Storm
Unpackage storm and add the bin/ directory to your PATH environment variable.

### Installing Redis
Install through your distribution's package manager.

***Ubuntu***

    $ sudo apt-get install redis-server

***Fedora***

    $ sudo yum install redis

Start up the redis server:

***Ubuntu****

    $ sudo service redis-server start

***Fedora***

    $ sudo systemctl start redis

## Compiling
Building using Maven.

    $ mvn package

## Set up Twitter's API Credentials
1. Create a Twitter application through [https://dev.twitter.com/](https://dev.twitter.com/).
2. Create an access token for your application.
3. Under `src/main/resources/` copy `twitter4j.sample.properties` to
   `twitter4j.properties` and configure with your OAuth consumer key, OAuth
   consumer secret, access token and access token secret.

## Running locally
Requires Redis to be running locally.

    $ storm jar target/big-data-project-1.0-SNAPSHOT-jar-with-dependencies.jar \
      edu.utdallas.cs.bigdataproject.TwitterTopology

## Checking top 10
Run the Redis CLI and use ZREVRANGE:

    $ redis-cli
      redis 127.0.0.1:6379> ZREVRANGE hashtagcounts 0 9

This will give you the top 10 hash tags by count. By default, this application
will run for 10 minutes and then stop.
