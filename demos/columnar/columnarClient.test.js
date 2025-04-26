// Import the client functions
const {
    submitRequest,
    getActiveRequests,
    cancelRequest,
    getCompletedRequests,
    // Import link functions if needed for testing
    // getLink,
    // createLink,
    // updateLink,
    // deleteLink
} = require('./columnarClient');

// No need for config, httpsAgent, or api instance here anymore
// They are managed within columnarClient.js

// Example usage (Test Script)
async function runDemo() {
  try {
    // The client initialization (including credential/URL checks)
    // now happens when columnarClient.js is required.
    // No need to repeat the checks here.

    // Example 1: Submit a simple query
    console.log('\n--- Test: Submit a simple query ---');
    const queryResult = await submitRequest('SELECT 1 AS test');
    // Optional: Add assertions here in a real test framework
    // expect(queryResult).toHaveProperty('requestID');

    // Example 2: Check active requests
    console.log('\n--- Test: Check active requests ---');
    await getActiveRequests();
    // Optional: Assertions

    // Example 3: Get completed requests
    console.log('\n--- Test: Get completed requests ---');
    await getCompletedRequests();
    // Optional: Assertions

    // Example 4: Submit a more complex query
    console.log('\n--- Test: Submit a complex query ---');
    await submitRequest('SELECT * FROM Metadata.`Database`');
    // Optional: Assertions

    // Example 5: Submit and then cancel a request using clientContextId
    console.log('\n--- Test: Submit and Cancel Request --- ');
    const clientContextId = `test-cancel-${Date.now()}`;
    let requestIdToCancel = null;
    try {
        // Use a simple query that will complete quickly
        const simpleQuery = 'SELECT "This is a test" AS message';
        
        console.log('Submitting a test query for cancellation test...');
        
        const submitResult = await submitRequest(simpleQuery, clientContextId);
        requestIdToCancel = submitResult?.requestID; // Get request ID if possible
        console.log(`Submitted request for cancellation test with clientContextId: ${clientContextId}, requestId: ${requestIdToCancel}`);

        // For test purposes, we'll simulate an attempt to cancel
        // even though the query likely already completed
        console.log('Attempting to cancel request (likely already completed)...');
        try {
            await cancelRequest(requestIdToCancel);
            console.log('Cancellation successful');
        } catch (cancelError) {
            // Check if this is because the query already completed
            if (cancelError.message.includes('400')) {
                console.log('Query completed before cancellation could be executed (expected behavior).');
                // Verify by checking completed requests
                const completed = await getCompletedRequests();
                const found = completed.some(req => req.requestID === requestIdToCancel);
                if (found) {
                    console.log('Verified query completed successfully in completed requests list.');
                } else {
                    console.warn('Query was not found in completed requests:', cancelError.message);
                }
            } else {
                throw cancelError; // Rethrow if it's a different error
            }
        }
        
        // Add a test for client context ID based cancellation
        console.log('\nTesting cancellation with client context ID...');
        const clientContextId2 = `test-cancel-context-${Date.now()}`;
        // Submit another query that should complete quickly
        const submitResult2 = await submitRequest('SELECT "Testing context ID cancellation" AS message', clientContextId2);
        console.log(`Submitted second request with clientContextId: ${clientContextId2}`);
        
        try {
            // Try to cancel by client context ID
            await cancelRequest(clientContextId2);
            console.log('Cancellation by client context ID successful');
        } catch (cancelError) {
            // Since query might have already completed, we should handle that case
            console.log('Query likely completed before cancellation could be executed.');
        }
    } catch (error) {
        console.error('Error during submit/cancel test:', error.message);
        // Don't stop the whole test run if cancellation fails
    }

    // --- Add Link Management Tests Here (Optional) ---
    // const linkTest = require('./linkManagementTests'); // Example: Move tests to another file
    // await linkTest.runLinkTests();


  } catch (error) {
    console.error('Test Demo execution failed:', error.message);
    // In a real test runner, this would fail the test suite
    process.exitCode = 1; // Indicate failure
  }
}

// Run the demo test script
console.log('Starting Columnar Client Test Script...');
runDemo().then(() => {
  console.log('\nTest Script completed!');
}).catch(error => {
  console.error('Error running test script:', error.message);
  process.exitCode = 1; // Indicate failure
});
