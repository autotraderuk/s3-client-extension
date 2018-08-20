# s3-client-extension for Java/Scala
![Travis (.org)](https://img.shields.io/travis/autotraderuk/s3-client-extension.svg)

*Disclaimer: This is an alpha release until we get continuous integration and pipelines in place. Use at your own risk!*
  
The **s3-client-extension** library enables Java and Scala developers to interact with the **Amazon Web Services** with ease.
It can be used to build scalable solutions with Amazon S3.
  
## Getting Started  
  
To get started you can simply download a single zip file or use Maven.  
  
s3-client-extension package is available for Scala 2.11.8 (on Java 8). To install the package using SBT,
add the below statement to your `build.sbt` file:
  
```
libraryDependencies += "uk.co.autotrader" %% "s3-client-extension" % "1.0.0-alpha.1" 
```
  
On maven, update your `pom.xml` file by adding the following to your dependencies:  

```html
<dependency>
	<groupId>uk.co.autotrader</groupId>
	<artifactId>s3-client-extension</artifactId>
	<version>1.0.0-alpha.1</version>
</dependency>
```

For gradle, update your `build.gradle` file by adding the following to your dependencies:
```
compile group: 'uk.co.autotrader', name: 's3-client-extension', version: '1.0.0-alpha.1'
```
## Example Usages

To get a fully paginated parallel list of object keys for objects in the specified bucket with the prefixes.
  
```scala
val bucket = "bucket-name"
val prefixes = List("prefix1/prefix2")
val keys = S3Client.withDefaultAmazonS3.getKeys(bucket, prefixes)
```

> **Note: the prefixes don't start with a slash.**

To get a string representation of the S3Path object.

```scala
val bucket = "bucket-name"
val objectPath = "test1/test2/test3.txt"
val s3Path = S3Path(bucket, objectPath).toString
```
> **Note: the object path does not start with a slash.**

## Dev Guidelines

If you're adding or changing something, submit your changes via a pull request and they will be reviewed.

## Semantic Versioning

This library follows semantic versioning and is managed by the **gradle-semantic-build-versioning** plugin. 
Please refer to [https://semver.org/](https://semver.org/) for more information about semantic versioning.

## Getting Help  
  
Please use GitHub issues for getting help. We monitor these to track for feature requests or bugs.  
    
## License  
Copyright (c) 2018 Auto Trader Limited

Published under Apache Software License 2.0, see LICENSE