# [WIP] Couchbase Connector for Google Looker Studio

This connector allows you to connect to a Couchbase database and run N1QL queries to visualize your data in Google Looker Studio.

## Features

- Connect to Couchbase Server or Couchbase Capella
- Support for buckets, scopes, and collections
- Run custom N1QL queries
- Secure authentication with PATH_USER_PASS

## Setup Instructions

1. Create a new Google Looker Studio report
2. Click "Create" and select "Data Source"
3. Search for "Couchbase" in the connector gallery or select it from the list
4. Enter your Couchbase connection details:
   - **Path**: Your Couchbase server URL (e.g., `couchbases://cb.example.cloud.couchbase.com` or `https://localhost:8091`)
   - **Username**: Your Couchbase username
   - **Password**: Your Couchbase password
5. Click "CONNECT"
6. Configure your query:
   - **Bucket Name**: The name of the Couchbase bucket to query
   - **Scope (Optional)**: The name of the scope within the bucket (defaults to `_default`)
   - **Collection (Optional)**: The name of the collection within the scope (defaults to `_default`)
   - **N1QL Query**: Enter a valid N1QL query that returns a consistent schema
7. Click "CONNECT" to create the data source

## Supported URL Formats

The connector supports the following URL formats for the Path field:

- **Couchbase Capella**: `couchbases://cb.<your-endpoint>.cloud.couchbase.com` or `https://cb.<your-endpoint>.cloud.couchbase.com`
- **Couchbase Server**: `couchbase://localhost:8091`, `couchbases://localhost:8091`, `https://localhost:8091`, or just `localhost:8091`

## N1QL Query Examples

Here are some example N1QL queries you can use:

```sql
-- Get all documents from a collection
SELECT * FROM `travel-sample` LIMIT 100

-- Query with scope and collection
SELECT * FROM `travel-sample`.inventory.airline LIMIT 100

-- Aggregate query
SELECT country, COUNT(*) as count 
FROM `travel-sample`.inventory.airline 
GROUP BY country 
ORDER BY count DESC

-- Join query
SELECT a.name, a.icao, r.sourceairport, r.destinationairport, r.equipment
FROM `travel-sample`.inventory.airline a
JOIN `travel-sample`.inventory.route r
ON r.airline = a.icao
LIMIT 100
```

## Troubleshooting

- **Authentication Error**: Make sure your username and password are correct and that the user has the necessary permissions to access the bucket.
- **Connection Error**: Verify that your Couchbase server URL is correct and accessible from the internet. For Capella, ensure you're using a secure connection (couchbases:// or https://).
- **Query Error**: Check your N1QL query syntax and make sure the bucket, scope, and collection exist.
- **Empty Results**: If your query returns no results, the connector will show an error. Modify your query to ensure it returns at least one row.

## Support

If you encounter any issues with this connector, please report them on the [GitHub issues page](https://github.com/googledatastudio/community-connectors/issues/new?title=Couchbase%20Connector%20Issue).

## License

This connector is licensed under the Apache License 2.0.
