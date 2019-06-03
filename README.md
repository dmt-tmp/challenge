
This repository contains a solution to the data engineer challenge. It was implemented using Scala and Spark, on macOS.

# Prerequisite

JDK 8 and sbt 1.x must be installed to build this project.

# How to build the project

```
# Compile source code
sbt compile

# Compile tests
sbt test:compile

# Run tests
sbt test

# Create an assembly jar
sbt assembly

```

# How to run the Spark job

Usage example:

```
spark-submit \
--driver-memory 2G \
--executor-memory 6G \
--executor-cores 3 \
--master spark://[master_ip]:7077 \
--class jp.paypay.challenge.SessionizationJob \
/path/to/challenge-assembly-0.1-SNAPSHOT.jar \
--access-logs-path /path/to/pp-challenge-instructions/data/2015_07_22_mktplace_shop_web_log_sample.log.gz \
--output-dir /tmp/sessionizationJobResults \
--nb-output-files 8 \
--new-session-threshold 15minutes
```

Read `jp.paypay.challenge.SessionizationJobArgs` for more info on arguments.

The Spark job:
- saves sessionized logs in parquet, in the directory defined by `--output-dir`.
- prints results for questions 2, 3 and 4

# Results

## Sessionization using "client_ip" and a window of 15 minutes

- The average session time is 574.472 seconds

- On average, a user visits 8 distinct URLs per session.

Here are more stats about the number of URL visits:

```
+-------+-----------------+
|summary|nb_url_visits    |
+-------+-----------------+
|count  |107756           |
|mean   |8.461199376368834|
|stddev |66.5119098316658 |
|min    |1                |
|max    |9532             |
+-------+-----------------+
```

- The 10 most engaged users, with their session time in seconds, are:

```
+------------+---------------+
|session_time|client_ip      |
+------------+---------------+
|4097        |220.226.206.7  |
|3042        |52.74.219.71   |
|3039        |119.81.61.166  |
|3029        |54.251.151.39  |
|3024        |103.29.159.186 |
|3009        |123.63.241.40  |
|3008        |202.134.59.72  |
|3007        |103.29.159.138 |
|3003        |115.184.198.199|
|3002        |122.169.141.4  |
+------------+---------------+
```

## Sessionization using "client_ip", "user_agent" and a window of 15 minutes

According to the challenge wording,

```
IP addresses do not guarantee distinct users, but this is the limitation of the data.
As a bonus, consider what additional data would help make better analytical conclusions.
```

In addition to field "client_ip", using the "user_agent" might help to distinguish users: this can be the case in companies, where several users have the same IP but use different browsers or different browser versions.

Here are the results

- The average session time is 532.311 seconds.

- URL visits:

```
+-------+-----------------+
|summary|nb_url_visits    |
+-------+-----------------+
|count  |121119           |
|mean   |7.67344512421668 |
|stddev |55.61681392732013|
|min    |1                |
|max    |9012             |
+-------+-----------------+
```

- Most engaged users:

```
+------------+---------------+----------------------------------------------------------------------------------------------------------------------+
|session_time|client_ip      |user_agent                                                                                                            |
+------------+---------------+----------------------------------------------------------------------------------------------------------------------+
|4097        |220.226.206.7  |-                                                                                                                     |
|3039        |119.81.61.166  |Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/534.55.3 (KHTML, like Gecko) Version/5.1.3 Safari/534.53.10|
|3036        |52.74.219.71   |Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)                                              |
|3029        |52.74.219.71   |-                                                                                                                     |
|3029        |54.251.151.39  |Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.8 (KHTML, like Gecko) Chrome/17.0.940.0 Safari/535.8              |
|3024        |103.29.159.186 |Mozilla/5.0 (Windows NT 6.1; rv:25.0) Gecko/20100101 Firefox/25.0                                                     |
|3007        |103.29.159.138 |Mozilla/5.0 (Windows NT 6.1; rv:21.0) Gecko/20100101 Firefox/21.0                                                     |
|3003        |54.251.151.39  |-                                                                                                                     |
|3003        |115.184.198.199|Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.134 Safari/537.36         |
|3002        |122.169.141.4  |Mozilla/5.0 (Windows NT 6.3; WOW64; rv:39.0) Gecko/20100101 Firefox/39.0                                              |
+------------+---------------+----------------------------------------------------------------------------------------------------------------------+
```

### Comparison of results

Using field "user_agent" in addition to "client_ip" results in a slightly smaller average session time and a slightly smaller number of URL visits.

Some client IPs, such as "52.74.219.71" and "54.251.151.39", appear twice in the most engaged users with different user agents, especially "-".

The 3rd most engaged user is actually Googlebot.
