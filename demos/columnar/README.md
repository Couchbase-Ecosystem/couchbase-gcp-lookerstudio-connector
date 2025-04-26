# Couchbase Columnar Client

A JavaScript client for interacting with Couchbase Columnar API.

## Environment Variables

Create a `.env` file in this directory with the following variables:

```
# Couchbase Columnar URL (without port)
CB_COLUMNAR_URL=https://your-instance.cloud.couchbase.com

# Couchbase Columnar Port
CB_COLUMNAR_PORT=18095

# Authentication credentials
CB_USERNAME=username
CB_PASSWORD=password
```

## Usage

```javascript
const {
  submitRequest,
  getActiveRequests,
  cancelRequest,
  getCompletedRequests,
  getLink,
  createLink,
  updateLink,
  deleteLink
} = require('./columnarClient');

// Submit a query
const result = await submitRequest('SELECT 1 AS test');

// Get active requests
const activeRequests = await getActiveRequests();

// Cancel a request (by request ID or client context ID)
await cancelRequest(requestId);

// Get completed requests
const completedRequests = await getCompletedRequests();
```

## Testing

Run the test script:

```
node columnarClient.test.js
``` 