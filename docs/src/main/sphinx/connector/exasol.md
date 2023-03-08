---
myst:
  substitutions:
    default_domain_compaction_threshold: '`32`'
---

# Exasol connector

```{raw} html
<img src="../_static/img/exasol.png" class="connector-logo">
```

The Exasol connector allows querying and creating tables in an external Exasol
database.

## Requirements

To connect to Exasol, you need:

* Exasol database version 7.1 or higher.
* Network access from the Trino coordinator and workers to Exasol.
  Port 8563 is the default port.

## Configuration

To configure the Exasol connector as the ``example`` catalog, create a file
named ``example.properties`` in ``etc/catalog``. Include the following
connection properties in the file:

```text
connector.name=exasol
connection-url=jdbc:exa:exasol.example.com:8563
connection-user=user
connection-password=secret
```

The ``connection-url`` defines the connection information and parameters to pass
to the JDBC driver. See the
[Exasol JDBC driver documentation](https://docs.exasol.com/db/latest/connect_exasol/drivers/jdbc.htm#ExasolURL)
for more information.

The ``connection-user`` and ``connection-password`` are typically required and
determine the user credentials for the connection, often a service user. You can
use {doc}`secrets </security/secrets>` to avoid using actual values in catalog
properties files.

:::{note}
If your Exasol database uses a self-signed TLS certificate you will must
specify the certificate's fingerprint in the JDBC URL using parameter
``fingerprint``, e.g.: ``jdbc:exa:exasol.example.com:8563;fingerprint=ABC123``.
:::

```{include} jdbc-authentication.fragment
```

```{include} jdbc-common-configurations.fragment
```

```{include} jdbc-domain-compaction-threshold.fragment
```

```{include} jdbc-case-insensitive-matching.fragment
```

## Querying Exasol

The Exasol connector provides a Trino schema for every Exasol schema.
You can see the available Exasol schema by running `SHOW SCHEMAS`:

```sql
SHOW SCHEMAS FROM example;
```

If you have a Exasol schema named `web`, you can view the tables
in this schema by running `SHOW TABLES`:

```sql
SHOW TABLES FROM example.web;
```

You can see a list of the columns in the `clicks` table in the `web`
schema using either of the following:

```sql
DESCRIBE example.web.clicks;
SHOW COLUMNS FROM example.web.clicks;
```

Finally, you can access the `clicks` table in the `web` schema:

```sql
SELECT * FROM example.web.clicks;
```

If you used a different name for your catalog properties file, use
that catalog name instead of `example` in the above examples.

:::{note}
The Exasol user must have access to the table in order to access it from Trino.
:::

(exasol-type-mapping)=

## Type mapping

Because Trino and Exasol each support types that the other does not, this
connector {ref}`modifies some types <type-mapping-overview>` when reading or
writing data. Data types may not map the same way in both directions between
Trino and the data source. Refer to the following sections for type mapping in
each direction.

### Exasol to Trino type mapping

Trino supports selecting Exasol database types. This table shows the Exasol to
Trino data type mapping:

```{eval-rst}
.. list-table:: Exasol to Trino type mapping
  :widths: 30, 25, 50
  :header-rows: 1

  * - Exasol database type
    - Trino type
    - Notes
  * - ``DECIMAL(p, s)``
    - ``DECIMAL(p, s)``
    -  See :ref:`exasol-number-mapping`
  * - ``DOUBLE PRECISION``
    - ``REAL``
    -
  * - ``VARCHAR(n)``
    - ``VARCHAR(n)``
    -
  * - ``CHAR(n)``
    - ``CHAR(n)``
    -
  * - ``DATE``
    - ``DATE``
    -
  * - ``TIMESTAMP``
    - ``TIMESTAMP``
    -
```

No other types are supported.

### Trino to Exasol type mapping

Trino supports creating tables with the following types in an Exasol database.
The table shows the mappings from Trino to Exasol data types:

```{eval-rst}
.. list-table:: Trino to Exasol Type Mapping
  :widths: 30, 25, 50
  :header-rows: 1

  * - Trino type
    - Exasol database type
    - Notes
  * - ``TINYINT``
    - ``DECIMAL(3, 0)``
    -
  * - ``SMALLINT``
    - ``DECIMAL(9, 0)``
    -
  * - ``INTEGER``
    - ``DECIMAL(18, 0)``
    -
  * - ``BIGINT``
    - ``DECIMAL(36 ,0)``
    -
  * - ``DECIMAL(p, s)``
    - ``DECIMAL(p, s)``
    -
  * - ``REAL``
    - ``DOUBLE PRECISION``
    -
  * - ``DOUBLE``
    - ``DOUBLE PRECISION``
    -
  * - ``VARCHAR``
    - ``VARCHAR(2000000)``
    -
  * - ``VARCHAR(n)``
    - ``VARCHAR(n)``
    - See :ref:`exasol-character-mapping`
  * - ``CHAR(n)``
    - ``CHAR(n)``
    - See :ref:`exasol-character-mapping`
  * - ``DATE``
    - ``DATE``
    -
  * - ``TIMESTAMP``
    - ``TIMESTAMP``
    -
```

No other types are supported.

(exasol-number-mapping)=
### Mapping numeric types

An Exasol `DECIMAL(p, s)` maps to Trino's `DECIMAL(p, s)` and vice versa
except in these conditions:

- No precision is specified for the column (example: `DECIMAL` or
  `DECIMAL(*)`).
- Scale (`s`) is greater than precision.
- Precision (`p`) is greater than 36.
- Scale is negative.

(exasol-character-mapping)=
### Mapping character types

Trino's `VARCHAR(n)` maps to `VARCHAR(n)` and vice versa if `n` is no greater
than 2000000. Exasol does not support longer values.
If no length is specified, the connector uses 2000000.

Trino's `CHAR(n)` maps to `CHAR(n)` and vice versa if `n` is no greater than 2000.
Exasol does not support longer values.

```{include} jdbc-type-mapping.fragment
```

(exasol-sql-support)=
## SQL support

The connector provides read access and write access to data and metadata in
Exasol. In addition to the {ref}`globally available <sql-globally-available>`
and {ref}`read operation <sql-read-operations>` statements, the connector
supports the following statements:

* {doc}`/sql/delete`
* {doc}`/sql/truncate`
* {doc}`/sql/create-table`
* {doc}`/sql/drop-table`
* {doc}`/sql/alter-table`
* {doc}`/sql/comment`

### SQL DELETE

If a `WHERE` clause is specified, the `DELETE` operation only works if the
predicate in the condition clause can be fully pushed down to the data source.

Exasol does not support predicates `<`, `>` and `!=` for the condition of
`DELETE` statements. The statement fails with a database error in these cases.

```{include} alter-table-limitation.fragment
```

## Table functions

The connector provides specific {doc}`table functions </functions/table>` to
access Exasol.

(exasol-query-function)=

### `query(varchar) -> table`

The `query` function allows you to query the underlying database directly. It
requires syntax native to Exasol, because the full query is pushed down and
processed in Exasol. This can be useful for accessing native features which are
not available in Trino or for improving query performance in situations where
running a query natively may be faster.

```{include} query-passthrough-warning.fragment
```

As a simple example, query the `example` catalog and select an entire table::

```sql
SELECT
  *
FROM
  TABLE(
    example.system.query(
      query => 'SELECT
        *
      FROM
        tpch.nation'
    )
  );
```

As a practical example, you can use the
[WINDOW clause from Exasol](https://docs.exasol.com/db/latest/sql_references/functions/analyticfunctions.htm#AnalyticFunctions):

```sql
SELECT
  *
FROM
  TABLE(
    example.system.query(
      query => 'SELECT
        id, department, hire_date, starting_salary,
        AVG(starting_salary) OVER w2 AVG,
        MIN(starting_salary) OVER w2 MIN_STARTING_SALARY,
        MAX(starting_salary) OVER (w1 ORDER BY hire_date)
      FROM employee_table
      WINDOW w1 as (PARTITION BY department), w2 as (w1 ORDER BY hire_date)
      ORDER BY department, hire_date'
    )
  );
```

```{include} query-table-function-ordering.fragment
```

## Performance

The connector includes a number of performance improvements, detailed in the
following sections.

(exasol-pushdown)=
### Pushdown

The connector supports pushdown for the following operations:

- {ref}`join-pushdown`

In addition, the connector supports {ref}`aggregation-pushdown` for the
following functions:

- {func}`avg()`
- {func}`count()`, also `count(distinct x)`
- {func}`max()`
- {func}`min()`
- {func}`sum()`

Pushdown is supported with the following functions:

* {func}`stddev()` and {func}`stddev_samp()`
* {func}`stddev_pop()`
* {func}`var_pop()`
* {func}`variance()` and {func}`var_samp()`
* {func}`covar_samp()`
* {func}`covar_pop()`

```{include} pushdown-correctness-behavior.fragment
```

```{include} join-pushdown-enabled-false.fragment
```