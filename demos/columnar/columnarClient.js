const axios = require('axios');
const https = require('https');
require('dotenv').config(); // Load environment variables from .env file

// Configuration
const config = {
  // Base URL is now preferentially loaded from .env, otherwise uses the default.
  baseUrl: process.env.CB_COLUMNAR_URL || 'https://localhost:18095', // Adjusted default for local testing
  auth: {
    // Credentials are now loaded from the .env file
    username: process.env.CB_USERNAME,
    password: process.env.CB_PASSWORD
  },
  // Allow overriding SSL verification via environment variable
  rejectUnauthorized: process.env.NODE_TLS_REJECT_UNAUTHORIZED !== '0' // Defaults to true unless overridden
};

// Validate required configuration
if (!config.auth.username || !config.auth.password) {
  console.error("FATAL ERROR: CB_USERNAME or CB_PASSWORD not found in .env file or environment variables.");
  process.exit(1); // Exit if critical config is missing
}
if (!config.baseUrl) {
  console.error("FATAL ERROR: CB_COLUMNAR_URL not found in .env file or environment variables.");
  process.exit(1); // Exit if critical config is missing
}

console.log(`Columnar Client Initializing with Base URL: ${config.baseUrl}`);
console.log(`Using Username: ${config.auth.username}`);
if (!config.rejectUnauthorized) {
    console.warn("Warning: Disabling SSL certificate verification (NODE_TLS_REJECT_UNAUTHORIZED=0). This is insecure and not recommended for production.");
}

// Create a custom HTTPS agent
const httpsAgent = new https.Agent({
  rejectUnauthorized: config.rejectUnauthorized
});

// Create axios instance with authentication and the custom HTTPS agent
const api = axios.create({
  baseURL: config.baseUrl, // Use baseURL for consistency
  auth: config.auth,
  headers: {
    'Content-Type': 'application/json'
  },
  timeout: 30000, // 30 seconds timeout
  httpsAgent // Use the custom HTTPS agent
});

// --- API Functions ---

// Submit a request (DDL, DML, Queries)
async function submitRequest(query, clientContextId = null) {
  try {
    const payload = { statement: query };
    if (clientContextId) {
      payload.client_context_id = clientContextId;
    }
    console.log(`Submitting request: ${JSON.stringify(payload)}`);
    const response = await api.post('/api/v1/request', payload);
    console.log('Request submission successful:', response.data);
    return response.data;
  } catch (error) {
    const errorMsg = error.response?.data || error.message;
    console.error('Error submitting request:', errorMsg);
    // Re-throw a more informative error if possible
    const customError = new Error(`Failed to submit request: ${JSON.stringify(errorMsg)}`);
    customError.originalError = error;
    throw customError;
  }
}

// Get active requests
async function getActiveRequests() {
  try {
    console.log('Getting active requests...');
    const response = await api.get('/api/v1/active_requests');
    console.log('Active requests received:', response.data);
    return response.data;
  } catch (error) {
    const errorMsg = error.response?.data || error.message;
    console.error('Error getting active requests:', errorMsg);
    const customError = new Error(`Failed to get active requests: ${JSON.stringify(errorMsg)}`);
    customError.originalError = error;
    throw customError;
  }
}

// Cancel a request by Request ID or Client Context ID
async function cancelRequest(identifier) {
  // Simple check: If it looks like a UUID, assume it's a Request ID
  const isRequestId = /[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}/.test(identifier);
  const endpoint = isRequestId ? `/api/v1/active_requests/${identifier}` : '/api/v1/request';
  const params = isRequestId ? {} : { client_context_id: identifier };

  try {
    console.log(`Attempting to cancel request with identifier: ${identifier} using endpoint: ${endpoint}`);
    const response = await api.delete(endpoint, { params });
    console.log('Cancel request successful:', response.data);
    return response.data;
  } catch (error) {
    const errorMsg = error.response?.data || error.message;
    console.error(`Error cancelling request ${identifier}:`, errorMsg);
    const customError = new Error(`Failed to cancel request ${identifier}: ${JSON.stringify(errorMsg)}`);
    customError.originalError = error;
    throw customError;
  }
}

// Get completed requests
async function getCompletedRequests() {
  try {
    console.log('Getting completed requests...');
    const response = await api.get('/api/v1/completed_requests');
    console.log('Completed requests received:', response.data);
    return response.data;
  } catch (error) {
    const errorMsg = error.response?.data || error.message;
    console.error('Error getting completed requests:', errorMsg);
    const customError = new Error(`Failed to get completed requests: ${JSON.stringify(errorMsg)}`);
    customError.originalError = error;
    throw customError;
  }
}

// --- Link Management Functions ---

async function getLink(name) {
  try {
    console.log(`Getting link: ${name}...`);
    const response = await api.get(`/api/v1/link/${name}`);
    console.log(`Link ${name} details received:`, response.data);
    return response.data;
  } catch (error) {
     const errorMsg = error.response?.data || error.message;
    console.error(`Error getting link ${name}:`, errorMsg);
    const customError = new Error(`Failed to get link ${name}: ${JSON.stringify(errorMsg)}`);
    customError.originalError = error;
    throw customError;
  }
}

async function createLink(name, linkConfig) {
  try {
    console.log(`Creating link: ${name} with config: ${JSON.stringify(linkConfig)}`);
    // Basic validation for Kafka link type
    if (linkConfig.type === 'kafka' && (!linkConfig.brokers || !linkConfig.topic)) {
        throw new Error('Missing required Kafka parameters: "brokers" and "topic"');
    }
    const response = await api.post(`/api/v1/link/${name}`, linkConfig);
    console.log(`Link ${name} creation successful:`, response.data);
    return response.data;
  } catch (error) {
    const errorMsg = error.response?.data || error.message;
    console.error(`Error creating link ${name}:`, errorMsg);
    const customError = new Error(`Failed to create link ${name}: ${JSON.stringify(errorMsg)}`);
    customError.originalError = error;
    throw customError;
  }
}

async function updateLink(name, linkConfig) {
  try {
     console.log(`Updating link: ${name} with config: ${JSON.stringify(linkConfig)}`);
     // Basic validation for Kafka link type
    if (linkConfig.type === 'kafka' && (!linkConfig.brokers || !linkConfig.topic)) {
        throw new Error('Missing required Kafka parameters: "brokers" and "topic"');
    }
    const response = await api.put(`/api/v1/link/${name}`, linkConfig);
    console.log(`Link ${name} update successful:`, response.data);
    return response.data;
  } catch (error) {
    const errorMsg = error.response?.data || error.message;
    console.error(`Error updating link ${name}:`, errorMsg);
    const customError = new Error(`Failed to update link ${name}: ${JSON.stringify(errorMsg)}`);
    customError.originalError = error;
    throw customError;
  }
}

async function deleteLink(name) {
  try {
    console.log(`Deleting link: ${name}...`);
    const response = await api.delete(`/api/v1/link/${name}`);
    // Couchbase might return 200 OK or 204 No Content on successful deletion
    console.log(`Link ${name} deletion successful (Status: ${response.status}):`, response.data || '(No content)');
    return response.data;
  } catch (error) {
    const errorMsg = error.response?.data || error.message;
    console.error(`Error deleting link ${name}:`, errorMsg);
    // If it's a 404, it might already be deleted, which isn't always an error in a delete operation
    if (error.response?.status === 404) {
        console.log(`Link ${name} not found, assumed already deleted.`);
        return { message: "Link not found, assumed already deleted." }; // Return a success-like object
    }
    const customError = new Error(`Failed to delete link ${name}: ${JSON.stringify(errorMsg)}`);
    customError.originalError = error;
    throw customError;
  }
}

// --- Export Functions ---
module.exports = {
  submitRequest,
  getActiveRequests,
  cancelRequest,
  getCompletedRequests,
  getLink,
  createLink,
  updateLink,
  deleteLink
}; 