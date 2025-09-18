### AltaworxPondProcessStagedDeviceStatus Lambda Flow Documentation

## Overview

The `AltaworxPondProcessStagedDeviceStatus` Lambda function consumes progress/page messages and processes staged Pond device-status records into final AMOP 2.0 tables. It validates and merges staged data, updates page-level processing status, and signals overall completion via database flags or optional SQS messages.

Shape

## HIGH-LEVEL FLOW (Sequential Function Flow)

### Main Entry Point

FunctionHandler (SQSEvent sqsEvent, ILambdaContext context)

- Receives SQS event and Lambda context
- Initializes base function handler
- Iterates through SQS records and routes per-message

### Initialization Flow (No ServiceProviderId supplied in SQS message)

InitializeProcessForPendingPages

- DiscoverPendingPagesToProcess (from DB tracking table)
- InitProcessStagedDeviceStatusPages (enqueue one SQS message per pending page)

Shape

### Processing Flow (ServiceProviderId supplied in SQS message)

ProcessPageByServiceProviderId

- FetchStagedDeviceStatusBatch (from staging table for SP/page)
- TransformAndValidate
- MergeDeviceStatusFromStaging (stored procedure call)
- UpdatePageStatusAndCheckProgress (page done/failed; overall check)
- Optionally emit completion message/metric

Shape

## LOW-LEVEL FLOW (Detailed Method Explanations)

### FunctionHandler (Main Entry Point)

- Input: `SQSEvent sqsEvent`, `ILambdaContext context`
- Purpose: Processes SQS messages to orchestrate staged device-status processing
- What happens:
  - Initializes `AmopLambdaContext` via `BaseAmopFunctionHandler()`
  - Reads environment variables via `TryGetAllEnvironmentVariables()`:
    - `POND_PROCESS_STAGED_DEVICE_STATUS_QUEUE_URL` (e.g., `PondHelper.CommonString.POND_PROCESS_STAGED_DEVICE_STATUS_QUEUE_URL_VARIABLE_KEY`)
    - `PAGE_SIZE` (optional; for batch/loop sizing, default from `PondHelper.CommonConfig.DEFAULT_PAGE_SIZE`)
  - Ensures SQS trigger validity and iterates each record
  - For each record:
    - Logs diagnostics
    - Parses attributes with `GetMessageValues()`:
      - `ServiceProviderId` (required for processing mode)
      - `PageNumber` (required for processing mode)
      - `IsSuccessful` (incoming flag from upstream; used in progress and auditing)
    - If `ServiceProviderId <= 0` or missing: routes to `InitializeProcessForPendingPages()`
    - Else: routes to `ProcessPageByServiceProviderId()`
  - Handles exceptions and calls `CleanUp()`

### InitializeProcessForPendingPages (Initialization Mode)

- Input: `AmopLambdaContext context`, `PondRepository pondRepository`
- Purpose: Seeds processing by discovering unprocessed/failed pages and fanning out SQS messages
- What happens:
  - Read pages pending device-status processing, e.g., from `POND_GET_DEVICE_STATUS_PAGE_TO_PROCESS` or reuse of `POND_GET_INVENTORIES_PAGE_TO_PROCESS` with a distinct "DeviceStatus" step/state
  - For each `serviceProviderId` and `pageNumber` pending:
    - `InitProcessStagedDeviceStatusPages(context, serviceProviderId, pageNumber)` enqueues SQS message to `ProcessStagedDeviceStatusQueueURL`

Shape

### ProcessPageByServiceProviderId (Processing Mode)

- Input: `AmopLambdaContext context`, `SqsValues sqsValues`
- Purpose: Processes one page of staged device-status records into final tables
- What happens:
  - Retrieve batch from staging via repository, e.g., `pondRepository.GetDeviceStatusStagingPage(serviceProviderId, pageNumber)`
  - `TransformAndValidate` (map to canonical schema; check required fields)
  - `MergeDeviceStatusFromStaging`:
    - Execute stored procedure(s), e.g., `UPDATE_POND_DEVICE_STATUS_FROM_STAGING`
    - Upsert device status into target tables
  - `UpdatePageStatusAndCheckProgress`:
    - Update page status (success/failure) in `..._PAGE_TO_PROCESS` tracking table
    - Optionally compute overall completion and set a final flag
  - Optionally publish completion/metric SQS message

Shape

## Utility Functions

### GetMessageValues

- Parses SQS attributes from the incoming message into `SqsValues`
- Attributes used (via `SQSMessageKeyConstant`):
  - `SERVICE_PROVIDER_ID`
  - `PAGE_NUMBER`
  - `IS_SUCCESSFUL` (incoming from upstream; propagated for auditing/telemetry)

### TryGetAllEnvironmentVariables

- Reads Lambda configuration from environment variables:
  - `POND_PROCESS_STAGED_DEVICE_STATUS_QUEUE_URL`
  - `PAGE_SIZE` (optional)

### InitializeRepositories

- Instantiates `PondRepository` and `ServiceProviderRepository` using `CentralDbConnectionString`

### FetchStagedDeviceStatusBatch

- Reads staged records for the specified service provider and page
- Typical source table: `PondDeviceStatusStaging` (or equivalent)

### MergeDeviceStatusFromStaging

- Uses SQL stored procedures to merge staged records into final tables
- Example procedures: `UPDATE_POND_DEVICE_STATUS_FROM_STAGING`, `UPSERT_POND_DEVICE_STATUS`
- Executes within SQL transient retry policy

### UpdatePageStatusAndCheckProgress

- Updates tracking table for the processed page: status, counts, error info
- Example table: `POND_GET_DEVICE_STATUS_PAGE_TO_PROCESS`
- Checks whether all pages for the run are complete; may set an overall completion indicator

### InitProcessStagedDeviceStatusPages

- Sends an SQS message per page to `ProcessStagedDeviceStatusQueueURL` with attributes:
  - `SERVICE_PROVIDER_ID`
  - `PAGE_NUMBER`

## Sync Orchestration

### SyncDeviceStatus (Coordinator)

- Purpose: Orchestrates fetch from staging, transformation, merge to final, and progress signaling using retry policy
- Steps:
  - `FetchStagedDeviceStatusBatch`
  - `TransformAndValidate`
  - `MergeDeviceStatusFromStaging`
  - `UpdatePageStatusAndCheckProgress`

Shape

## Key Dependencies and Integrations

- **AwsFunctionBase**: logging, config, DB connections, cleanup
- **PondRepository**: DB CRUD for staging and progress tracking
- **ServiceProviderRepository**: service provider metadata (if needed)
- **EnvironmentRepository**: environment variable access
- **SqsService**: SQS message publishing (fan-out and/or completion)
- **RetryPolicyHelper**: SQL transient retry policy

Shape

## Data Flow Summary

- **Input**: SQS message identifies `ServiceProviderId` and `PageNumber`
- **Stage Read**: Read staged device-status rows for that page from `PondDeviceStatusStaging`
- **Merge**: Execute stored procedure(s) to upsert into final tables
- **Advance**: Update page status and optionally emit completion metrics/messages

Note: Page-to-process tracking is recorded in a `..._PAGE_TO_PROCESS` table; repository methods (e.g., `UpdateDeviceStatusPageStatusAndCheckSyncProgress`) update status and compute completion where applicable.

Shape

## AltaworxPondProcessStagedDeviceStatus — Integration & Operations Guide

1) Triggers & Scheduling

- **Trigger**: SQS event sourced from upstream (e.g., inventory/device-status staging producers)
- **Publisher of initial SQS messages**: Upstream Lambda(s) or this Lambda in initialization mode
- **EventBridge schedule**: Not required; optional if you want periodic draining of pending pages
- **Cron**: N/A by default
- **Time zone**: UTC
- **Frequency**: On-demand via SQS; optional schedule if configured

2) Message Handling

- **SQS message attributes (page messages)**:
  - `SERVICE_PROVIDER_ID`
  - `PAGE_NUMBER`
  - `IS_SUCCESSFUL` (from upstream; used for audit/telemetry only)
- **Continuation for pagination**: One SQS message per page; each message processes exactly one page
- **Manual/default invocation**: If the event has no `SERVICE_PROVIDER_ID`, the Lambda discovers pending pages in the DB tracking table and enqueues page messages

3) Batch & Pagination

- **Batch unit**: Page-level batches driven by the same page markers used by the staging producer
- **Pagination mechanics**: No HTTP pagination; uses DB page markers from `..._PAGE_TO_PROCESS`
- **Completion determination**: After each page is merged, the DB page status is updated; when all pages are complete, the run is considered complete

4) Integration Details (Authentication)

- **Credential source**: Database connectivity via `CentralDbConnectionString`
- **External API calls**: None in this Lambda; it operates on staged data

5) Data Handling & Staging

- **Staging tables**:
  - `PondDeviceStatusStaging` (device-status records staged by upstream)
- **Tracking tables**:
  - `POND_GET_DEVICE_STATUS_PAGE_TO_PROCESS` (or shared page-tracking table with a distinct step/state)
- **Final sync to AMOP 2.0 tables**:
  - Performed by stored procedures, e.g., `UPDATE_POND_DEVICE_STATUS_FROM_STAGING`

6) Error Handling & Retry

- **SQL retries (Polly)**:
  - Attempts: Config-driven (e.g., `CommonConstants.NUMBER_OF_RETRIES`)
  - Backoff: Exponential (e.g., `API_ERROR_DELAY_IN_SECONDS^attempt`)
- **DB failures**: Logged; page marked unsuccessful; progress recorded in tracking table
- **Re-enqueue of incomplete jobs**: Not handled automatically; retried by next scheduled/init run or operational tooling

7) Failed/Unprocessed Records

- **Validation failures**: Records can be skipped or diverted during merge based on stored procedure logic
- **Failure logging**: CloudWatch logs; page-level success/failure captured in DB tracking
- **Retry policy**: Via subsequent runs; no per-record retry in this Lambda

8) Cleanup Processes

- **Retention (DaysToKeep)**: Not implemented in this Lambda
- **Cleanup batch size (RecordsPerCycle)**: Not implemented in this Lambda
- **Cleanup logging**: Not applicable

9) Notifications & Reporting

- **Notifications**: None beyond CloudWatch logs
- **Sync summary reports**: Not produced here; progress/status captured in DB

10) External Dependencies (Prerequisites)

- **Environment variables**:
  - `POND_PROCESS_STAGED_DEVICE_STATUS_QUEUE_URL`
  - `PAGE_SIZE` (optional)
- **Infrastructure**:
  - SQS queue for “process staged device status”
  - DB connectivity for staging and final merge
- **Permissions**:
  - IAM permissions to read from SQS, write logs, access DB (via Secrets Manager/SSM as applicable)

## Sample SQS Message (Page Processing)

```json
{
  "MessageAttributes": {
    "SERVICE_PROVIDER_ID": { "StringValue": "123", "DataType": "Number" },
    "PAGE_NUMBER": { "StringValue": "5", "DataType": "Number" },
    "IS_SUCCESSFUL": { "StringValue": "true", "DataType": "String" }
  },
  "MessageBody": "Process staged device status for SP 123 page 5"
}
```

## Operational Notes

- Ensure upstream staging producers complete before or concurrently with this Lambda’s processing
- Monitor the tracking table for page-level statuses and overall completion
- Use initialization mode to re-fanout processing for pages in a failed or pending state

Shape