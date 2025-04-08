// testClient.js

// Import the client functions
const {
    submitRequest,
    getActiveRequests,
    cancelRequest,
    getCompletedRequests,
    getLink,
    createLink,
    updateLink,
    deleteLink
} = require('./couchbaseColumnarClient');

// --- Configuration --- 
// !!! IMPORTANT: Replace this with your actual Couchbase Columnar API endpoint !!!
// const API_BASE_URL = 'http://YOUR_COUCHBASE_API_ENDPOINT'; 
const API_BASE_URL = 'http://localhost:8095'; 
// Example authorization (uncomment and adapt if needed)
// const AUTH_TOKEN = 'Bearer your_token_here'; 
// You would need to modify the client functions to accept and use headers like Authorization

// --- Credentials --- 
// !!! IMPORTANT: Replace these with your actual Couchbase credentials !!!
const CB_USERNAME = 'kaustav'; // Replace with your Couchbase username
const CB_PASSWORD = 'password'; // Replace with your Couchbase password

// --- Test Functions --- 

async function testSubmitAndTrack() {
    console.log('\n--- Testing Submit Request ---');
    // Define a unique client context ID for this request
    const clientContextId = `test-${Date.now()}`;
    // Replace with your actual query or DDL/DML
    const requestBody = { 
        statement: "SELECT 1;",
        client_context_id: clientContextId // Include the client context ID
        // Example DDL: statement: "CREATE SCOPE myBucket.`_default`.myNewScope;"
        // Example DML: statement: "INSERT INTO myBucket.`_default`.myScope (KEY, VALUE) VALUES (UUID(), {'type':'test'});"
    };
    let requestId = null;
    try {
        const result = await submitRequest(requestBody, CB_USERNAME, CB_PASSWORD, API_BASE_URL);
        console.log('Submit Result:', JSON.stringify(result, null, 2));
        // Extract the actual request ID from the response
        requestId = result.requestID;
        console.log(`Submitted request with ID: ${requestId}`);
        console.log(`Using clientContextId: ${clientContextId}`);
    } catch (error) {
        console.error('Failed to submit request:', error.message);
        return; // Stop if submission fails
    }

    if (!requestId) {
        console.warn('Could not determine request ID from submit response. Skipping active/cancel tests.');
        return;
    }

    console.log('\n--- Testing Get Active Requests ---');
    try {
        // Small delay to allow request to potentially become active
        await new Promise(resolve => setTimeout(resolve, 1000)); 
        const activeRequests = await getActiveRequests(CB_USERNAME, CB_PASSWORD, API_BASE_URL);
        console.log('Active Requests:', JSON.stringify(activeRequests, null, 2));
        // Check if our request ID is in the list (assuming active requests are an array of objects with requestID)
        let isActive = false;
        if (Array.isArray(activeRequests)) {
             isActive = activeRequests.some(req => req.requestID === requestId || req.clientContextID === clientContextId);
        } else {
            console.warn('Response from getActiveRequests was not an array, cannot check if request is active.');
        }
        console.log(`Is request ${requestId} active? ${isActive}`);
    } catch (error) {
        console.error('Failed to get active requests:', error.message);
    }

    // Optional: Test cancellation (be cautious with this)
    console.log('\n--- Testing Cancel Request ---');
    try {
        // Use clientContextId to cancel
        const cancelResult = await cancelRequest(clientContextId, CB_USERNAME, CB_PASSWORD, API_BASE_URL);
        console.log('Cancel Result:', JSON.stringify(cancelResult, null, 2));
    } catch (error) {
        console.error(`Failed to cancel request using clientContextId ${clientContextId}:`, error.message);
    }
}

async function testCompletedRequests() {
     console.log('\n--- Testing Get Completed Requests ---');
    try {
        const completedRequests = await getCompletedRequests(CB_USERNAME, CB_PASSWORD, API_BASE_URL);
        console.log('Completed Requests:', JSON.stringify(completedRequests, null, 2));
    } catch (error) {
        console.error('Failed to get completed requests:', error.message);
    }
}

async function testLinkManagement() {
    const linkName = `testLink_${Date.now()}`; // Unique name for testing
    // Replace with actual link configuration (e.g., for Kafka)
    // !!! IMPORTANT: You MUST provide the correct configuration for your Kafka link below,
    //              based on the Couchbase documentation (https://docs.couchbase.com/server/current/analytics-rest-links/index.html#manage-kafka-links)
    //              This likely includes `brokers`, `topic`, and potentially authentication details.
    const linkConfig = { 
        type: "kafka",
        // Example REQUIRED parameters (replace with your actual values):
        // "brokers": "your-kafka-broker:9092",
        // "topic": "your-kafka-topic",
        // Example OPTIONAL parameters:
        // "useSSL": false,
        // "saslMechanism": "PLAIN", // or SCRAM-SHA-256, SCRAM-SHA-512
        // "username": "your-kafka-user",
        // "password": "your-kafka-password", 
        description: "A temporary test link"
    };
    const updatedLinkConfig = { ...linkConfig, description: "Updated test link description" };

    console.log(`\n--- Testing Link Management (using link name: ${linkName}) ---`);

    try {
        console.log('\nAttempting to create link...');
        const createResult = await createLink(linkName, linkConfig, CB_USERNAME, CB_PASSWORD, API_BASE_URL);
        console.log('Create Link Result:', JSON.stringify(createResult, null, 2));
    } catch (error) {
        console.error(`Failed to create link ${linkName}:`, error.message);
        return; // Stop if creation fails
    }

    try {
        console.log('\nAttempting to get link...');
        const getResult = await getLink(linkName, CB_USERNAME, CB_PASSWORD, API_BASE_URL);
        console.log('Get Link Result:', JSON.stringify(getResult, null, 2));
    } catch (error) {
        console.error(`Failed to get link ${linkName}:`, error.message);
    }

    try {
        console.log('\nAttempting to update link...');
        const updateResult = await updateLink(linkName, updatedLinkConfig, CB_USERNAME, CB_PASSWORD, API_BASE_URL);
        console.log('Update Link Result:', JSON.stringify(updateResult, null, 2));
        
        // Verify update
        console.log('\nVerifying update by getting link again...');
        const getUpdatedResult = await getLink(linkName, CB_USERNAME, CB_PASSWORD, API_BASE_URL);
        console.log('Get Updated Link Result:', JSON.stringify(getUpdatedResult, null, 2));

    } catch (error) {
        console.error(`Failed to update link ${linkName}:`, error.message);
    }

    try {
        console.log('\nAttempting to delete link...');
        const deleteResult = await deleteLink(linkName, CB_USERNAME, CB_PASSWORD, API_BASE_URL);
        console.log('Delete Link Result:', JSON.stringify(deleteResult, null, 2));

        // Verify deletion (should fail with 404 or similar)
        console.log('\nVerifying deletion by getting link again (expecting error)...');
        await getLink(linkName, CB_USERNAME, CB_PASSWORD, API_BASE_URL); 
    } catch (error) {
         console.log(`Successfully verified link deletion (received error): ${error.message}`);
         // console.error(`Failed to delete link ${linkName}:`, error.message); // Uncomment if you don't expect an error here
    }
}


// --- Main Execution --- 
async function runTests() {
    console.log(`Starting tests against: ${API_BASE_URL}`);
    if (API_BASE_URL.includes('YOUR_COUCHBASE_API_ENDPOINT')) {
        console.warn('\n!!! WARNING: API_BASE_URL is set to the placeholder value. Tests will likely fail. Edit testClient.js to set the correct URL. !!!\n');
    }
    if (CB_USERNAME === 'YOUR_USERNAME' || CB_PASSWORD === 'YOUR_PASSWORD') {
         console.warn('\n!!! WARNING: Credentials are set to placeholder values. Tests will likely fail. Edit testClient.js to set the correct username and password. !!!\n');
    }
    
    await testSubmitAndTrack();
    await testCompletedRequests(); // Re-enable this test
    // await testLinkManagement(); // Keep commented unless Kafka config is added

    console.log('\n--- Tests Complete ---');
}

runTests().catch(error => {
    console.error('\n--- An unexpected error occurred during test execution: ---', error);
}); 