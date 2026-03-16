# Java API Reference

The NeuG Java API provides a Java-native driver for connecting to NeuG servers, executing Cypher queries, and consuming typed query results.

## Overview

The Java driver is designed for application integration and service-side usage:

- **Create drivers** to connect to a NeuG server over HTTP
- **Open sessions** to execute Cypher queries
- **Read results** through a typed `ResultSet` API
- **Inspect metadata** using native NeuG `Types`

## Installation

### Use from this repository

```bash
cd tools/java_driver
mvn clean install -DskipTests
```

### Add dependency in another Maven project

```xml
<dependency>
	<groupId>com.alibaba.neug</groupId>
	<artifactId>neug-java-driver</artifactId>
	<version>1.0.0-SNAPSHOT</version>
</dependency>
```

## Core Interfaces

- **Driver** - manages connectivity and creates sessions
- **Session** - executes statements against a NeuG server
- **ResultSet** - reads rows and typed values from query results
- **ResultSetMetaData** - inspects result column names, nullability, and native NeuG types

## Quick Start

```java
import com.alibaba.neug.driver.Driver;
import com.alibaba.neug.driver.GraphDatabase;
import com.alibaba.neug.driver.ResultSet;
import com.alibaba.neug.driver.Session;

public class Example {
	public static void main(String[] args) {
		try (Driver driver = GraphDatabase.driver("http://localhost:10000")) {
			driver.verifyConnectivity();

			try (Session session = driver.session();
					ResultSet rs = session.run("RETURN 1 AS value")) {
				while (rs.next()) {
					System.out.println(rs.getInt("value"));
				}
			}
		}
	}
}
```

## Configuration

You can create a driver with custom connection settings through `Config`:

```java
import com.alibaba.neug.driver.Driver;
import com.alibaba.neug.driver.GraphDatabase;
import com.alibaba.neug.driver.utils.Config;

Config config = Config.builder()
		.withConnectionTimeoutMillis(3000)
		.build();

Driver driver = GraphDatabase.driver("http://localhost:10000", config);
```

## Parameterized Queries

```java
import java.util.HashMap;
import java.util.Map;

Map<String, Object> parameters = new HashMap<>();
parameters.put("name", "Alice");
parameters.put("age", 30);

try (Session session = driver.session()) {
	String query = "CREATE (p:Person {name: $name, age: $age}) RETURN p";
	try (ResultSet rs = session.run(query, parameters)) {
		if (rs.next()) {
			System.out.println(rs.getObject("p"));
		}
	}
}
```

## Reading Results

The Java driver exposes typed accessors for common value types:

- `getString(...)`
- `getInt(...)`
- `getLong(...)`
- `getBoolean(...)`
- `getDate(...)`
- `getTimestamp(...)`
- `getObject(...)`

Example:

```java
try (Session session = driver.session();
		ResultSet rs = session.run("MATCH (n:Person) RETURN n.name AS name, n.age AS age")) {
	while (rs.next()) {
		String name = rs.getString("name");
		int age = rs.getInt("age");
		System.out.println(name + ", " + age);
	}
}
```

## Result Metadata

`ResultSetMetaData` returns native NeuG types instead of JDBC SQL type codes.

```java
ResultSetMetaData metaData = rs.getMetaData();
String columnName = metaData.getColumnName(0);
Types columnType = metaData.getColumnType(0);
String typeName = metaData.getColumnTypeName(0);
```

This is useful when building higher-level abstractions on top of the driver or when dispatching logic based on result types.

## Dependencies

The Java driver depends on the following libraries:

- OkHttp - HTTP client
- Protocol Buffers - response serialization
- Jackson - JSON processing
- SLF4J - logging facade

These dependencies are managed automatically by Maven.

## API Documentation

- [Generated Javadoc](./apidocs/index.html)

## Build Javadoc Locally

```bash
cd tools/java_driver
mvn -DskipTests javadoc:javadoc
```

The generated Javadoc is written to `tools/java_driver/target/site/apidocs`.