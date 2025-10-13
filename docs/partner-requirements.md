# Looker Studio Partner Connector Requirements

## Table of Contents

1. [Overview](#overview)
2. [Manifest Requirements](#manifest-requirements)
3. [OAuth Configuration](#oauth-configuration)
4. [URL Fetch Whitelist](#url-fetch-whitelist)
5. [OAuth Verification Process](#oauth-verification-process)
6. [Deployment Checklist](#deployment-checklist)
7. [Common Issues & Solutions](#common-issues--solutions)
8. [Review Submission](#review-submission)

---

## Overview

This document details the requirements for publishing Couchbase connectors to the Looker Studio Partner Connector Gallery. These requirements are based on Google's Partner Connector (PSCC) requirements and our experience resolving the initial review feedback.

### Official Requirements Documentation

- [Partner Connector Requirements](https://developers.google.com/looker-studio/connector/pscc-requirements)
- [Manifest Reference](https://developers.google.com/looker-studio/connector/manifest)
- [OAuth Scopes Documentation](https://developers.google.com/apps-script/concepts/scopes)
- [Apps Script Client Verification](https://developers.google.com/apps-script/guides/client-verification)

---

## Manifest Requirements

### Required Fields in `appsscript.json`

The `dataStudio` object in your manifest must include the following fields:

#### 1. Basic Information

```json
{
  "dataStudio": {
    "name": "Connector Name",
    "company": "Couchbase",
    "companyUrl": "https://www.couchbase.com/",
    "logoUrl": "https://toppng.com/uploads/preview/couchbase-logo-11609358583zeesg8fjx7.png"
  }
}
```

**Requirements:**

- `name`: The display name of your connector
- `company`: Your organization name
- `companyUrl`: Your company's website
- `logoUrl`: Publicly accessible logo URL (should be square, 120px � 120px recommended)

#### 2. Documentation Links

```json
{
  "dataStudio": {
    "addonUrl": "https://developer.couchbase.com/tutorial-looker-studio-columnar/",
    "supportUrl": "https://github.com/Couchbase-Ecosystem/couchbase-gcp-lookerstudio-connector/issues"
  }
}
```

**Requirements:**

- `addonUrl`: Link to connector documentation or tutorial
- `supportUrl`: **MUST be a hosted page** (not an email or mailto link) where users can report issues or get support
- Both URLs should be on authorized domains (same as companyUrl domain when possible)

#### 3. Descriptions

```json
{
  "dataStudio": {
    "shortDescription": "Connect to Couchbase Columnar to visualize and analyze your data with custom queries",
    "description": "Connect to and visualize data from Couchbase Server or Capella. This connector supports N1QL queries, scopes and collections, vector search (7.6+), and advanced query options for optimal performance."
  }
}
```

**Requirements:**

- `shortDescription`:
  - **Maximum 125 characters**
  - **Cannot contain URLs**
  - Must be concise and descriptive
  - Free of spelling and grammatical errors
- `description`: Longer detailed description of connector functionality

#### 4. Fee Type (REQUIRED)

```json
{
  "dataStudio": {
    "feeType": ["FREE"]
  }
}
```

**Options:**

- `["FREE"]` - For connectors that don't charge
- `["PAID"]` - For connectors that require payment

**Note:** This field was missing from our initial submission and caused rejection.

#### 5. Legal Links (REQUIRED)

```json
{
  "dataStudio": {
    "privacyPolicyUrl": "https://www.couchbase.com/privacy-policy/",
    "termsOfServiceUrl": "https://www.couchbase.com/terms-of-use/"
  }
}
```

**Requirements:**

- Both URLs are **mandatory** for Partner Connector submission
- Should be on the **same domain** as `addonUrl` (or at least on an authorized domain)
- Must be publicly accessible
- Should be valid, current legal documents

**Couchbase URLs:**

- Privacy Policy: https://www.couchbase.com/privacy-policy/
- Terms of Service: https://www.couchbase.com/terms-of-use/
- Master Service Agreement: https://www.couchbase.com/msa/

#### 6. Sources and Authentication

```json
{
  "dataStudio": {
    "sources": ["COUCHBASE_COLUMNAR"],
    "authType": ["PATH_USER_PASS"]
  }
}
```

**Note:** These were already correctly configured in our connectors.

### Complete Manifest Examples

#### Columnar Connector

```json
{
  "timeZone": "Asia/Kolkata",
  "dependencies": {},
  "exceptionLogging": "STACKDRIVER",
  "runtimeVersion": "V8",
  "dataStudio": {
    "name": "Couchbase Columnar",
    "logoUrl": "https://toppng.com/uploads/preview/couchbase-logo-11609358583zeesg8fjx7.png",
    "company": "Couchbase",
    "companyUrl": "https://www.couchbase.com/",
    "addonUrl": "https://developer.couchbase.com/tutorial-looker-studio-columnar/",
    "supportUrl": "https://github.com/Couchbase-Ecosystem/couchbase-gcp-lookerstudio-connector/issues",
    "shortDescription": "Connect to Couchbase Columnar to visualize and analyze your data with custom queries",
    "description": "Connect to and visualize data from Couchbase Server or Capella. This connector supports N1QL queries, scopes and collections, vector search (7.6+), and advanced query options for optimal performance.",
    "feeType": ["FREE"],
    "privacyPolicyUrl": "https://www.couchbase.com/privacy-policy/",
    "termsOfServiceUrl": "https://www.couchbase.com/terms-of-use/",
    "sources": ["COUCHBASE_COLUMNAR"],
    "authType": ["PATH_USER_PASS"]
  },
  "oauthScopes": ["https://www.googleapis.com/auth/script.external_request"],
  "urlFetchWhitelist": ["https://*.cloud.couchbase.com/api/"]
}
```

#### DataApi Connector

```json
{
  "timeZone": "Asia/Kolkata",
  "dependencies": {},
  "exceptionLogging": "STACKDRIVER",
  "runtimeVersion": "V8",
  "dataStudio": {
    "name": "Couchbase DataApi",
    "logoUrl": "https://toppng.com/uploads/preview/couchbase-logo-11609358583zeesg8fjx7.png",
    "company": "Couchbase",
    "companyUrl": "https://www.couchbase.com/",
    "addonUrl": "https://developer.couchbase.com/tutorial-looker-studio-dataapi/",
    "supportUrl": "https://github.com/Couchbase-Ecosystem/couchbase-gcp-lookerstudio-connector/issues",
    "shortDescription": "Connect to Couchbase Server to visualize your data with N1QL queries and analytics",
    "description": "Connect to and visualize data from Couchbase Server or Capella. This connector supports N1QL queries, scopes and collections, vector search (7.6+), and advanced query options for optimal performance.",
    "feeType": ["FREE"],
    "privacyPolicyUrl": "https://www.couchbase.com/privacy-policy/",
    "termsOfServiceUrl": "https://www.couchbase.com/terms-of-use/",
    "sources": ["COUCHBASE"],
    "authType": ["PATH_USER_PASS"]
  },
  "oauthScopes": ["https://www.googleapis.com/auth/script.external_request"],
  "urlFetchWhitelist": ["https://*.cloud.couchbase.com/_p/"]
}
```

---

## OAuth Configuration

### OAuth Scopes

#### 1. Manifest Declaration

All OAuth scopes used by your connector must be explicitly declared in the `oauthScopes` array:

```json
{
  "oauthScopes": ["https://www.googleapis.com/auth/script.external_request"]
}
```

#### 2. Why `script.external_request` is Required

This scope is **mandatory** for connectors that use `UrlFetchApp.fetch()` to make HTTP requests to external services.

**What it does:**

- Allows Apps Script to make HTTP requests to external URLs
- Displayed to users as "Connect to an external service"
- Classified by Google as a "sensitive scope"

**Our connectors use UrlFetchApp in:**

- Columnar Connector: 14+ fetch calls to Couchbase Columnar API
- DataApi Connector: Multiple fetch calls to Couchbase query service

#### 3. OAuth Consent Screen Configuration

**Critical:** The scopes in your manifest **MUST match exactly** with the scopes configured in the Google Cloud Console OAuth consent screen.

### Setting Up OAuth Consent Screen

#### Step 1: Navigate to OAuth Consent Screen

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Select your project: `cb-columnar-lookerstudio-proj`
3. Navigate to: **APIs & Services** � **OAuth consent screen** � **Branding**

#### Step 2: Configure App Information

Fill in all required fields:

```
App name: Couchbase (or specific connector name)
User support email: your-email@couchbase.com
App logo: Upload Couchbase logo (JPG/PNG/BMP, max 1MB, 120px�120px recommended)
```

#### Step 3: Configure App Domain

All three fields are **required**:

```
Application home page: https://www.couchbase.com/
Application privacy policy link: https://www.couchbase.com/privacy-policy/
Application terms of service link: https://www.couchbase.com/terms-of-use/
```

#### Step 4: Configure Authorized Domains

Add the top-level domains used in your app:

```
couchbase.com
github.com (optional, for supportUrl)
```

**Note:** Adding `couchbase.com` automatically authorizes all subdomains:

-  www.couchbase.com
-  developer.couchbase.com
-  docs.couchbase.com

**Common Error:** You cannot add subdomains directly (e.g., `developer.couchbase.com` will fail with "Invalid domain: must be a top private domain")

#### Step 5: Add OAuth Scopes (CRITICAL)

1. Navigate to: **APIs & Services** � **OAuth consent screen** � **Data Access**
2. Click "Add or remove scopes"
3. The `script.external_request` scope will **NOT** appear in the standard list
4. Scroll to "Manually add scopes" section at the bottom
5. Enter the scope: `https://www.googleapis.com/auth/script.external_request`
6. Click "Add to table"
7. Click "Update"
8. **IMPORTANT:** Click "Save" on the main Data Access page

**Result:** The scope will appear under "Your sensitive scopes" section:

```
Scope: .../auth/script.external_request
User-facing description: Connect to an external service
```

#### Step 6: Save All Changes

Click the "Save" button at the bottom of each page to persist your changes.

---

## URL Fetch Whitelist

### Overview

The `urlFetchWhitelist` property restricts which external URLs your connector can access via `UrlFetchApp.fetch()`.

### Requirements

- **Must be included** for Partner Connectors (or exemption must be documented)
- Must list **all endpoints** used with `UrlFetchApp`
- Should use **least permissive scope** possible
- Helps protect user data by limiting external access

### Format Requirements

#### Valid Format

```json
{
  "urlFetchWhitelist": [
    "https://*.example.com/api/",
    "https://specific.domain.com/path/"
  ]
}
```

**Requirements:**

1. Must use `https://` (not http)
2. Must include domain
3. Must have **non-empty path** (trailing slash alone is considered empty)
4. Can use **single wildcard** (`*`) for subdomains only

#### Wildcard Rules

 **VALID:**

- `https://*.cloud.couchbase.com/api/` - Matches any subdomain with `/api/` path

L **INVALID:**

- `https://*.*.example.com/foo` - Multiple wildcards forbidden
- `https://subdomain.*.example.com/foo` - Wildcard must be leading prefix
- `https://*.cloud.couchbase.com` - Missing non-empty path (will error)
- `https://*.cloud.couchbase.com/` - Path with only `/` considered empty by validation

### Our Implementation

#### Why Wildcards Are Necessary

Couchbase Capella uses **dynamic, user-specific subdomains**:

- Format: `cb.{unique-id}.cloud.couchbase.com`
- Examples:
  - `cb.abc123xyz.cloud.couchbase.com`
  - `cb.def456uvw.cloud.couchbase.com`
- Each user has a different subdomain assigned by Capella infrastructure
- Subdomains cannot be predetermined or enumerated

#### Connector-Specific Implementations

**Columnar Connector:**

```json
{
  "urlFetchWhitelist": ["https://*.cloud.couchbase.com/api/"]
}
```

- **Endpoint used:** `/api/v1/request`
- **Matches:** `https://cb.{any-id}.cloud.couchbase.com/api/v1/request`
- **Security:** Restricts access to Capella API endpoints only

**DataApi Connector:**

```json
{
  "urlFetchWhitelist": ["https://*.cloud.couchbase.com/_p/"]
}
```

- **Endpoint used:** `/_p/query/query/service`
- **Matches:** `https://cb.{any-id}.cloud.couchbase.com/_p/query/query/service`
- **Security:** Restricts access to Capella private endpoints only

### URL Matching Behavior

- **Prefix matching:** Matches URLs that start with the whitelist entry
- **Path matching:** Case-sensitive
- **Child paths:** Automatically matched (e.g., `/api/` matches `/api/v1/request`)

### Exception Documentation Template

If questioned about urlFetchWhitelist implementation, use this explanation:

```
Explanation for urlFetchWhitelist Implementation:

This connector includes urlFetchWhitelist with wildcard subdomain matching:
`https://*.cloud.couchbase.com/api/` (Columnar) and
`https://*.cloud.couchbase.com/_p/` (DataApi).

Rationale for Wildcard Usage:

1. Dynamic User-Specific Subdomains: Each Couchbase Capella user has a
   unique, dynamically-assigned cluster subdomain (e.g.,
   cb.abc123xyz.cloud.couchbase.com, cb.def456uvw.cloud.couchbase.com).
   These subdomains are generated by Couchbase's cloud infrastructure and
   cannot be predetermined or enumerated.

2. Fixed Base Domain: All connections are restricted to the official
   Couchbase Capella domain (*.cloud.couchbase.com), ensuring users can
   only connect to verified Couchbase infrastructure. No arbitrary external
   URLs are accessible.

3. Specific API Paths: The whitelist further restricts access to specific
   API endpoints:
   - Columnar Connector: Only /api/v1/request endpoint (matches /api/ prefix)
   - DataApi Connector: Only /_p/query/query/service endpoint (matches /_p/ prefix)

4. Security Considerations: The wildcard applies only to the subdomain, not
   the domain or path. This provides the minimum necessary scope while
   maintaining securityusers can only access their own Couchbase Capella
   clusters through official API endpoints.

Alternative Approach Not Feasible: Omitting urlFetchWhitelist entirely would
be overly permissive. Using wildcards provides appropriate security boundaries
while accommodating the dynamic nature of cloud-hosted database clusters.
```

---

## OAuth Verification Process

### Understanding the "Unverified App Screen"

#### What is it?

When users authorize your connector, they may see a warning screen:

```
This app isn't verified
This app hasn't been verified by Google yet. Only proceed if you know and
trust the developer.
```

#### Why does it appear?

1. Your connector requests sensitive scopes (`script.external_request`)
2. OAuth verification has not been completed
3. User is external to your organization

#### Why "Verification is not required" is misleading

The OAuth console may show "Verification is not required" but this only applies to:

- Internal users within your Google Workspace organization
- Test/development environments

**For external users (Looker Studio customers), verification IS required** to remove the warning.

### How to Complete OAuth Verification

#### Prerequisites

1.  Domain ownership verified (couchbase.com)
2.  Standard Google Cloud Project (not Apps Script default project)
3.  All OAuth scopes added to consent screen
4.  Branding information complete (logo, links, descriptions)

#### Verification Requirements

**Required Assets:**

- Application name
- Application logo (JPEG/PNG/BMP, d1MB)
- Support email
- List of OAuth scopes used
- Authorized domains
- Application homepage URL
- Privacy policy URL
- Terms of service URL

#### Verification Steps

1. **Verify Domain Ownership**

   - Navigate to [Google Search Console](https://search.google.com/search-console)
   - Add property: `couchbase.com`
   - Complete domain verification (DNS record or file upload)
   - Domain owner must be an editor/owner of the Apps Script project

2. **Configure OAuth Consent Screen**

   - Complete all sections in Branding page
   - Add all required scopes to Data Access page
   - Ensure scopes match those in your manifest exactly
   - Save all changes

3. **Submit for Verification**

   - Navigate to: OAuth consent screen � Verification Center
   - Click "Submit for verification"
   - Fill in all required information
   - Submit application

4. **Wait for Review**

   - Processing time: **24-72 hours** (typically)
   - Google will review your application
   - May request additional information

5. **Test After Approval**
   - **CRITICAL:** Test with a **NEW Google account** (not your developer account)
   - Verify that "Unverified app screen" does NOT appear
   - This confirms verification is working for external users

#### Common Verification Issues

**Issue 1: Scope Mismatch**

- **Problem:** Manifest scopes don't match OAuth consent screen scopes
- **Solution:** Ensure `script.external_request` is added to Data Access section
- **Symptom:** "Unverified app" appears even after verification

**Issue 2: Developer Account Testing**

- **Problem:** Testing with your own developer account
- **Result:** You won't see the "Unverified app" screen (even if it shows to others)
- **Solution:** Always test with a fresh, external Google account

**Issue 3: Incomplete Branding**

- **Problem:** Missing required fields in OAuth consent screen
- **Result:** Verification request rejected
- **Solution:** Complete ALL fields in Branding section

---

## Deployment Checklist

### Pre-Deployment Verification

#### 1. Manifest Validation

- [ ] All required fields present in `dataStudio` object
- [ ] `feeType` specified
- [ ] `shortDescription` under 125 characters, no URLs
- [ ] `privacyPolicyUrl` and `termsOfServiceUrl` included
- [ ] `urlFetchWhitelist` properly formatted with non-empty path
- [ ] `oauthScopes` includes `script.external_request`
- [ ] No syntax errors in JSON

#### 2. OAuth Configuration

- [ ] App name configured
- [ ] App logo uploaded
- [ ] Application home page URL added
- [ ] Privacy policy URL added
- [ ] Terms of service URL added
- [ ] Authorized domains added (couchbase.com)
- [ ] `script.external_request` scope added to Data Access
- [ ] All changes saved

#### 3. Code Verification

- [ ] All `UrlFetchApp.fetch()` calls use URLs matching `urlFetchWhitelist`
- [ ] No hardcoded credentials
- [ ] Error handling implemented
- [ ] Logging statements appropriate for production

#### 4. Testing

- [ ] Connector works with test Capella cluster
- [ ] Authentication flow completes successfully
- [ ] Data retrieval works correctly
- [ ] Schema inference returns valid fields
- [ ] No errors in execution logs

### Deployment Steps

#### 1. Update Manifest Files

```bash
# Update both connector manifests
src/columnar/appsscript.json
src/dataapi/appsscript.json
```

#### 2. Deploy to Apps Script

**For each connector:**

1. Open Apps Script project in browser
2. Copy updated `appsscript.json` content
3. Paste into project's manifest file
4. Save changes
5. Create new deployment:
   - Click "Deploy" � "New deployment"
   - Select type: "Looker Studio Connector"
   - Add description: "Version X.X - Partner Connector requirements"
   - Click "Deploy"
6. **Copy deployment ID** (format: `AKfycby...`)

#### 3. Test Deployed Connectors

**For each connector:**

1. Open Looker Studio
2. Create new data source
3. Search for "Couchbase" or use deployment URL
4. Complete authentication:
   - Enter Capella cluster URL
   - Enter credentials
   - Click "Authenticate"
5. Verify authorization screen shows correct scope:
   - "Connect to an external service"
6. Complete configuration
7. Verify data loads correctly

#### 4. Document Deployment IDs

**Columnar Connector:**

```
Deployment ID: AKfycby...
Deployment Date: YYYY-MM-DD
Version: X.X
```

**DataApi Connector:**

```
Deployment ID: AKfycby...
Deployment Date: YYYY-MM-DD
Version: X.X
```

---

## Common Issues & Solutions

### Issue 1: "urlFetchWhitelist is missing from the manifest"

**Cause:** The `urlFetchWhitelist` property was not included in the manifest.

**Solution:**

```json
{
  "urlFetchWhitelist": ["https://*.cloud.couchbase.com/api/"]
}
```

---

### Issue 2: "URLs must have a non-empty path"

**Error Message:**

```
"appsscript.json" has errors: URLs must have a non-empty path:
https://*.cloud.couchbase.com
```

**Cause:** URL in `urlFetchWhitelist` doesn't include a path after the domain.

**Incorrect:**

```json
"urlFetchWhitelist": ["https://*.cloud.couchbase.com"]
"urlFetchWhitelist": ["https://*.cloud.couchbase.com/"]
```

**Correct:**

```json
"urlFetchWhitelist": ["https://*.cloud.couchbase.com/api/"]
```

**Key Point:** The path must be **truly non-empty** - a single `/` is considered empty.

---

### Issue 3: "feeType is missing from the manifest"

**Cause:** The `feeType` field was not included in the `dataStudio` object.

**Solution:**

```json
{
  "dataStudio": {
    "feeType": ["FREE"]
  }
}
```

---

### Issue 4: "privacyPolicyUrl is missing from the manifest"

**Cause:** Legal URLs not included in manifest.

**Solution:**

```json
{
  "dataStudio": {
    "privacyPolicyUrl": "https://www.couchbase.com/privacy-policy/",
    "termsOfServiceUrl": "https://www.couchbase.com/terms-of-use/"
  }
}
```

---

### Issue 5: "supportUrl page does not provide a way for users to report an issue"

**Cause:** `supportUrl` points to email (mailto:) or non-functional page.

**Incorrect:**

```json
"supportUrl": "mailto:support@couchbase.com"
"supportUrl": "https://couchbase.com/contact"
```

**Correct:**

```json
"supportUrl": "https://github.com/Couchbase-Ecosystem/couchbase-gcp-lookerstudio-connector/issues"
```

---

### Issue 6: "The connector is showing the Unverified app screen"

**Root Cause:** OAuth scope not added to consent screen or verification not complete.

**Solution:**

1. Navigate to OAuth consent screen � Data Access
2. Click "Add or remove scopes"
3. Manually add: `https://www.googleapis.com/auth/script.external_request`
4. Click "Save"
5. Submit for OAuth verification
6. Test with external Google account after approval

---

### Issue 7: "Invalid domain: must be a top private domain"

**Cause:** Trying to add subdomain instead of top-level domain.

**Incorrect:** `developer.couchbase.com`
**Correct:** `couchbase.com` (automatically includes all subdomains)

---

### Issue 8: Scope appears in "Sensitive" instead of "Non-sensitive"

**Status:** This is CORRECT and expected!

Google classifies `script.external_request` as a sensitive scope because it allows connection to external services.

---

## Review Submission

### Submission Checklist

Before submitting:

- [ ] All manifest fields completed
- [ ] Both connectors deployed with new deployment IDs
- [ ] OAuth consent screen fully configured
- [ ] OAuth scopes match manifest
- [ ] All URLs tested and accessible
- [ ] Connector functionality tested end-to-end
- [ ] Documentation updated
- [ ] Deployment IDs documented

### Submission Message Template

```
Dear Looker Studio Review Team,

We have addressed all issues identified in the previous review:

1.  feeType: Added "FREE" fee type to both connectors
2.  shortDescription: Added concise descriptions under 125 characters
3.  privacyPolicyUrl: Added https://www.couchbase.com/privacy-policy/
4.  termsOfServiceUrl: Added https://www.couchbase.com/terms-of-use/
5.  urlFetchWhitelist: Added with wildcard for dynamic Capella subdomains
6.  supportUrl: Verified GitHub issues page provides user support
7.  OAuth Verification: Scope added to consent screen, verification pending

Connectors:
- Couchbase Columnar: AKfycby... (deployed YYYY-MM-DD)
- Couchbase DataApi: AKfycby... (deployed YYYY-MM-DD)

All changes have been tested and verified.

Best regards,
[Your Name]
```

---

## Additional Resources

### Official Documentation

- [Looker Studio Community Connectors](https://developers.google.com/looker-studio/connector)
- [Partner Connector Requirements](https://developers.google.com/looker-studio/connector/pscc-requirements)
- [Manifest Reference](https://developers.google.com/looker-studio/connector/manifest)
- [URL Allowlisting](https://developers.google.com/apps-script/manifest/allowlist-url)

### Couchbase Resources

- [Privacy Policy](https://www.couchbase.com/privacy-policy/)
- [Terms of Use](https://www.couchbase.com/terms-of-use/)
- [Developer Portal](https://developer.couchbase.com/)

---

_Last updated: January 2025_
_Maintained by: Couchbase Engineering Team_
