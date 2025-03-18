# Couchbase Connector for Looker Studio

This connector allows you to connect to a Couchbase database and visualize your data in Google Looker Studio.

## Features

- Connect to a Couchbase database using username/password authentication
- Support for both self-managed Couchbase Server and Couchbase Capella (cloud)
- Run N1QL queries against your Couchbase buckets, scopes, and collections
- Advanced query options including scan consistency and timeout settings
- Vector search support for Couchbase Server 7.6+
- Visualize the results in Looker Studio

## Setup and Configuration

### Prerequisites

- A Couchbase Server instance (version 7.0 or higher recommended)
- A Couchbase user with appropriate permissions to run N1QL queries
- The Couchbase Query Service must be enabled

### Configuration Parameters

When using this connector in Looker Studio, you'll need to provide the following information:

1. **Couchbase Server URL**: 
   - For self-managed: `https://localhost:8091` or `https://your-couchbase-server:8091`
   - For Capella: `https://cb.<your-endpoint>.cloud.couchbase.com`

2. **Bucket Name**: The name of the Couchbase bucket you want to query (e.g., `travel-sample`)

3. **Scope (Optional)**: The name of the scope within the bucket (e.g., `inventory`). Defaults to `_default` if not specified.

4. **Collection (Optional)**: The name of the collection within the scope (e.g., `airport`). Defaults to `_default` if not specified.

5. **N1QL Query**: A valid N1QL query that returns the data you want to visualize

### Authentication

This connector uses basic username/password authentication. You'll need to provide:

1. **Username**: Your Couchbase username
2. **Password**: Your Couchbase password

For Capella, use the database user credentials created in the Capella UI.

## Connecting to Couchbase Capella

To connect to Couchbase Capella:

1. Log in to your Capella account and navigate to your cluster
2. Copy the connection string from the Connect tab
3. Use the hostname part of the connection string (e.g., `cb.<your-endpoint>.cloud.couchbase.com`) as the Server URL
4. Use the database username and password created in Capella
5. Specify the bucket, scope, and collection as needed

## N1QL Query Tips

- Your N1QL query should return a consistent schema (same fields for all rows)
- Use field aliases to make your column names more readable in Looker Studio
- Limit the number of rows returned to improve performance
- Use appropriate indexes to optimize your queries

Example query for the travel-sample bucket:

```sql
SELECT 
  meta().id as document_id,
  name,
  type,
  address.city as city,
  address.country as country,
  ARRAY_LENGTH(reviews) as review_count,
  rating
FROM `travel-sample`.inventory.airport 
WHERE country = "United States" 
LIMIT 100
```

## Working with Collections

Couchbase 7.0+ organizes documents into collections within scopes. This connector supports specifying the scope and collection to query. The query context is automatically set based on the bucket, scope, and collection you specify.

For example, if you specify:
- Bucket: `travel-sample`
- Scope: `inventory`
- Collection: `airport`

The connector will set the query context to `travel-sample.inventory.airport`.

## Advanced Query Options

### Scan Consistency

The connector supports two scan consistency options:

1. **Not Bounded (Default)**: Provides the fastest performance but may not include the most recent mutations.
2. **Request Plus**: Ensures that the query results include all data that was written before the query was issued, at the cost of potentially higher latency.

### Query Timeout

You can specify a custom timeout value (in milliseconds) for your queries. This is useful for complex queries that might take longer to execute. The default timeout is 30,000 milliseconds (30 seconds).

## Vector Search (Couchbase Server 7.6+)

Couchbase Server 7.6 introduced vector search capabilities, which this connector supports. To use vector search:

1. Enable vector search in the connector configuration
2. Specify the field containing vector embeddings
3. Ensure your Couchbase Server has vector search enabled and properly configured

Vector search allows you to perform similarity searches on vector embeddings, which is useful for applications like semantic search, recommendation systems, and image similarity.

Example vector search query:

```sql
SELECT meta().id, name, description, vector_score() AS score
FROM `travel-sample`.inventory.airport
WHERE vector_match(embeddings_field, $vector_query, {"num_candidates": 100})
ORDER BY vector_score() DESC
LIMIT 10
```

Note: Vector embeddings must be generated externally and stored in your Couchbase documents before they can be used for vector search.

## Limitations

- The connector currently supports only basic authentication
- Queries are executed directly against the Couchbase server, so performance depends on your query complexity and data size
- The connector does not support parameterized queries at this time

## Troubleshooting

If you encounter issues:

1. Verify your Couchbase server URL is correct and accessible
2. Ensure your username and password are valid
3. Test your N1QL query directly in the Couchbase Query Workbench
4. Check that your query returns results with a consistent schema
5. For Capella connections, ensure you're using HTTPS and the correct endpoint

## License

This connector is licensed under the Apache License 2.0.
