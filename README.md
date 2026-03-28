# JottQL Database Engine

## Overview

JottQL is a lightweight command-line database engine that allows users to create, manage, and query relational data. It supports basic SQL-like operations including table creation, insertion, querying, updating, and deletion. The system also optionally supports B+ tree indexing for improved query performance.

---

## Building and Running the Database

To start the database, run:

```
java JottQL <dbLocation> <pageSize> <bufferSize> <indexing>
```

### Parameters

* **dbLocation (String)**
  Path to the database directory. Can be absolute or relative.

* **pageSize (Integer)**
  Size of each page in bytes.

* **bufferSize (Integer)**
  Number of pages the buffer can hold.

* **indexing (Boolean)**
  Determines whether B+ tree indexing is enabled (`true` or `false`).

---

## Supported Commands

Once the database is running, you can use the following command-line operations:

---

### CREATE TABLE

```
CREATE TABLE <table_name> (<attributes>);
```

* **table_name**: Name of the new table (must be unique)
* **attributes**: Comma-separated list of attribute definitions
  Format: `<attribute_name> <type>`

---

### DROP TABLE

```
DROP TABLE <table_name>;
```

* **table_name**: Name of the table to delete

---

### ALTER TABLE

```
ALTER TABLE <table_name> <ADD | DROP> <attribute>;
```

* **table_name**: Table to modify
* **ADD / DROP**: Operation to perform
* **attribute**:

  * If **DROP**: attribute name
  * If **ADD**: `<attribute_name> <type>`

---

### SELECT

```
SELECT <attributes> FROM <tables> WHERE <conditions>;
```

* **attributes**:
  Comma-separated list of attributes to retrieve

  * Use `*` for all attributes
  * Use `<table>.<attribute>` if ambiguous

* **tables**:
  Comma-separated list of tables

  * Multiple tables are combined using a Cartesian product

* **conditions**:
  Equality condition to filter rows

---

### INSERT

```
INSERT <table_name> VALUES (<values>);
```

* **table_name**: Table to insert into
* **values**:
  Comma-separated list of tuples

  * Each tuple is space-separated
  * Must match table attribute order

---

### DELETE

```
DELETE FROM <table_name> WHERE <conditions>;
```

* **table_name**: Table to delete from
* **conditions**: Equality condition to filter rows

---

### UPDATE

```
UPDATE <table_name> SET <attribute> WHERE <conditions>;
```

* **table_name**: Table to update
* **attribute**: Attribute name and new value
* **conditions**: Equality condition to filter rows

---

## Supported Data Types

* **INTEGER**
  Non-negative integer values

* **DOUBLE**
  Non-negative double-precision floating-point values

* **BOOLEAN**
`True` or `False`

* **CHAR(N)**
  Fixed-length string of exactly N characters

* **VARCHAR(N)**
  Variable-length string with a maximum of N characters

