/**
 * Submits a DDL, DML, or query request. The requestBody object can include a `client_context_id` property.
 * @param {object} requestBody - The request payload (DDL, DML, or query). Can include `client_context_id`.
 * @param {string} [username] - Optional username for Basic Auth.
 * @param {string} [password] - Optional password for Basic Auth.
 * @param {string} [baseUrl=''] - The base URL of the API.
 * @returns {Promise<object>} - The response from the API.
 */
async function submitRequest(requestBody, username, password, baseUrl = '') {
    const url = `${baseUrl}/analytics/service`;
    const headers = {
        'Content-Type': 'application/json',
    };

    if (username && password) {
        const credentials = Buffer.from(`${username}:${password}`).toString('base64');
        headers['Authorization'] = `Basic ${credentials}`;
    }

    try {
        const response = await fetch(url, {
            method: 'POST',
            headers: headers,
            body: JSON.stringify(requestBody),
        });
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status} ${await response.text()}`);
        }
        return await response.json();
    } catch (error) {
        console.error('Error submitting request:', error);
        throw error;
    }
}

/**
 * Gets the list of active requests.
 * @param {string} [username] - Optional username for Basic Auth.
 * @param {string} [password] - Optional password for Basic Auth.
 * @param {string} [baseUrl=''] - The base URL of the API.
 * @returns {Promise<object>} - The list of active requests.
 */
async function getActiveRequests(username, password, baseUrl = '') {
    const url = `${baseUrl}/analytics/admin/active_requests`;
     const headers = {};

    if (username && password) {
        const credentials = Buffer.from(`${username}:${password}`).toString('base64');
        headers['Authorization'] = `Basic ${credentials}`;
    }

    try {
        const response = await fetch(url, {
            method: 'GET',
            headers: headers,
        });
        if (!response.ok) {
             throw new Error(`HTTP error! status: ${response.status} ${await response.text()}`);
        }
        return await response.json();
    } catch (error) {
        console.error('Error getting active requests:', error);
        throw error;
    }
}

/**
 * Cancels an active request using its client_context_id.
 * @param {string} clientContextId - The client_context_id of the request to cancel.
 * @param {string} [username] - Optional username for Basic Auth.
 * @param {string} [password] - Optional password for Basic Auth.
 * @param {string} [baseUrl=''] - The base URL of the API.
 * @returns {Promise<object>} - The response from the API.
 */
async function cancelRequest(clientContextId, username, password, baseUrl = '') {
    const url = `${baseUrl}/analytics/admin/active_requests`;
    const headers = {
        // Required content type for form data
        'Content-Type': 'application/x-www-form-urlencoded',
    };

    if (username && password) {
        const credentials = Buffer.from(`${username}:${password}`).toString('base64');
        headers['Authorization'] = `Basic ${credentials}`;
    }

    // Encode the client_context_id as form data
    const body = new URLSearchParams();
    body.append('client_context_id', clientContextId);

    try {
        const response = await fetch(url, {
            method: 'DELETE',
            headers: headers,
            body: body, // Send form data in the body
        });
        if (!response.ok && response.status !== 204) { // Allow 204 No Content
            // Handle expected statuses like 404 Not Found if the request is already gone
            throw new Error(`HTTP error! status: ${response.status} ${await response.text()}`);
        }
        // DELETE might return no content (204) or a confirmation
        if (response.status === 204) {
            return { success: true, message: 'Request cancelled successfully.' };
        }
        // If not 204, try to parse JSON, otherwise return success (e.g., for 200/202 status)
        const contentType = response.headers.get("content-type");
        if (contentType && contentType.indexOf("application/json") !== -1) {
             return await response.json();
        } else {
             return { success: true, status: response.status };
        }
    } catch (error) {
        console.error('Error cancelling request:', error);
        throw error;
    }
}

/**
 * Gets the list of completed requests.
 * @param {string} [username] - Optional username for Basic Auth.
 * @param {string} [password] - Optional password for Basic Auth.
 * @param {string} [baseUrl=''] - The base URL of the API.
 * @returns {Promise<object>} - The list of completed requests.
 */
async function getCompletedRequests(username, password, baseUrl = '') {
    const url = `${baseUrl}/analytics/admin/completed_requests`;
     const headers = {};

    if (username && password) {
        const credentials = Buffer.from(`${username}:${password}`).toString('base64');
        headers['Authorization'] = `Basic ${credentials}`;
    }

    try {
        const response = await fetch(url, {
            method: 'GET',
            headers: headers,
        });
        if (!response.ok) {
             throw new Error(`HTTP error! status: ${response.status} ${await response.text()}`);
        }
        return await response.json();
    } catch (error) {
        console.error('Error getting completed requests:', error);
        throw error;
    }
}

/**
 * Gets configuration for a specific link.
 * @param {string} name - The name of the link.
 * @param {string} [username] - Optional username for Basic Auth.
 * @param {string} [password] - Optional password for Basic Auth.
 * @param {string} [baseUrl=''] - The base URL of the API.
 * @returns {Promise<object>} - The link configuration.
 */
async function getLink(name, username, password, baseUrl = '') {
    const url = `${baseUrl}/analytics/link/${encodeURIComponent(name)}`;
     const headers = {};

    if (username && password) {
        const credentials = Buffer.from(`${username}:${password}`).toString('base64');
        headers['Authorization'] = `Basic ${credentials}`;
    }

    try {
        const response = await fetch(url, {
            method: 'GET',
            headers: headers,
        });
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status} ${await response.text()}`);
        }
        return await response.json();
    } catch (error) {
        console.error(`Error getting link ${name}:`, error);
        throw error;
    }
}

/**
 * Creates a new link.
 * @param {string} name - The name for the new link.
 * @param {object} linkConfig - The configuration for the link.
 * @param {string} [username] - Optional username for Basic Auth.
 * @param {string} [password] - Optional password for Basic Auth.
 * @param {string} [baseUrl=''] - The base URL of the API.
 * @returns {Promise<object>} - The response from the API.
 */
async function createLink(name, linkConfig, username, password, baseUrl = '') {
    const url = `${baseUrl}/analytics/link/${encodeURIComponent(name)}`;
     const headers = {
        'Content-Type': 'application/json',
    };

    if (username && password) {
        const credentials = Buffer.from(`${username}:${password}`).toString('base64');
        headers['Authorization'] = `Basic ${credentials}`;
    }

    try {
        const response = await fetch(url, {
            method: 'POST',
            headers: headers,
            body: JSON.stringify(linkConfig),
        });
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status} ${await response.text()}`);
        }
        return await response.json();
    } catch (error) {
        console.error(`Error creating link ${name}:`, error);
        throw error;
    }
}

/**
 * Updates an existing link.
 * @param {string} name - The name of the link to update.
 * @param {object} linkConfig - The updated configuration for the link.
 * @param {string} [username] - Optional username for Basic Auth.
 * @param {string} [password] - Optional password for Basic Auth.
 * @param {string} [baseUrl=''] - The base URL of the API.
 * @returns {Promise<object>} - The response from the API.
 */
async function updateLink(name, linkConfig, username, password, baseUrl = '') {
    const url = `${baseUrl}/analytics/link/${encodeURIComponent(name)}`;
      const headers = {
        'Content-Type': 'application/json',
    };

    if (username && password) {
        const credentials = Buffer.from(`${username}:${password}`).toString('base64');
        headers['Authorization'] = `Basic ${credentials}`;
    }

    try {
        const response = await fetch(url, {
            method: 'PUT',
            headers: headers,
            body: JSON.stringify(linkConfig),
        });
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status} ${await response.text()}`);
        }
        return await response.json();
    } catch (error) {
        console.error(`Error updating link ${name}:`, error);
        throw error;
    }
}

/**
 * Deletes a link.
 * @param {string} name - The name of the link to delete.
 * @param {string} [username] - Optional username for Basic Auth.
 * @param {string} [password] - Optional password for Basic Auth.
 * @param {string} [baseUrl=''] - The base URL of the API.
 * @returns {Promise<object>} - The response from the API.
 */
async function deleteLink(name, username, password, baseUrl = '') {
    const url = `${baseUrl}/analytics/link/${encodeURIComponent(name)}`;
    const headers = {};

    if (username && password) {
        const credentials = Buffer.from(`${username}:${password}`).toString('base64');
        headers['Authorization'] = `Basic ${credentials}`;
    }

    try {
        const response = await fetch(url, {
            method: 'DELETE',
            headers: headers,
        });
         if (!response.ok && response.status !== 204) { // Allow 204 No Content
             // Handle expected statuses like 404 Not Found if the link doesn't exist
            throw new Error(`HTTP error! status: ${response.status} ${await response.text()}`);
        }
         // DELETE might return no content (204) or a confirmation
        if (response.status === 204) {
            return { success: true, message: `Link ${name} deleted successfully.` };
        }
         // If not 204, try to parse JSON, otherwise return success (e.g., for 200/202 status)
        const contentType = response.headers.get("content-type");
        if (contentType && contentType.indexOf("application/json") !== -1) {
             return await response.json();
        } else {
             return { success: true, status: response.status };
        }
    } catch (error) {
        console.error(`Error deleting link ${name}:`, error);
        throw error;
    }
}


// Export functions if using in a module environment (e.g., Node.js)
// Ensure fetch is available in your environment (Node.js 18+ or use a polyfill like node-fetch)
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