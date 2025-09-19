## AltaworxPondProcessStagedDeviceStatus Lambda Flow Documentation

### Overview
The AltaworxPondProcessStagedDeviceStatus Lambda consumes SQS messages to process staged Pond device-status records into the final AMOP 2.0 tables. It can run in two modes:
- **Initialization mode**: Discovers pending pages to process and fans out SQS messages for each page.
- **Processing mode**: Processes a single page identified by `ServiceProviderId` and `PageNumber` by fetching staged rows, transforming/validating them, merging into final tables via stored procedures, and updating progress tracking.

The function leverages common infrastructure (logging, configuration, DB connections, SQS) provided by `AwsFunctionBase` and repository classes.

---

### High-Level Flow
1. **Main Entry**: `FunctionHandler(SQSEvent sqsEvent, ILambdaContext context)`
   - Initializes base context and configuration.
   - Iterates SQS records.
   - Routes to initialization or page processing based on message attributes.
2. **Initialization Flow (no ServiceProviderId)**: `InitializeProcessForPendingPages(...)`
   - Discovers pending pages to process from a DB tracking table.
   - Enqueues one SQS message per page to the processing queue.
3. **Processing Flow (with ServiceProviderId)**: `ProcessPageByServiceProviderId(...)`
   - Fetches staged rows for the given service provider and page.
   - Transforms and validates data.
   - Merges into final tables via stored procedures.
   - Updates page status and checks overall completion.
   - Optionally emits completion messages/metrics.

---

### Configuration and Environment Variables
- **POND_PROCESS_STAGED_DEVICE_STATUS_QUEUE_URL**
  - SQS queue URL used for fan-out and processing messages.
  - Example key: `PondHelper.CommonString.POND_PROCESS_STAGED_DEVICE_STATUS_QUEUE_URL_VARIABLE_KEY`.
- **PAGE_SIZE** (optional)
  - Overrides default page/batch size used by repository queries.
  - Default: `PondHelper.CommonConfig.DEFAULT_PAGE_SIZE`.
- Additional shared configuration (via `AwsFunctionBase`):
  - Database connection strings (e.g., Central DB) for `PondRepository` and `ServiceProviderRepository`.
  - Logging/telemetry configuration.

---

### SQS Message Contract (Attributes)
Attributes are parsed by `GetMessageValues()` using `SQSMessageKeyConstant` keys:
- **SERVICE_PROVIDER_ID** (number)
- **PAGE_NUMBER** (number)
- **IS_SUCCESSFUL** (boolean/string flag propagated from upstream for auditing)

Behavioral routing:
- If `SERVICE_PROVIDER_ID` is missing or <= 0 → Initialization mode.
- Otherwise → Processing mode for that specific `(ServiceProviderId, PageNumber)`.

---

### Detailed Method Documentation

#### FunctionHandler(SQSEvent sqsEvent, ILambdaContext context)
- **Purpose**: Lambda entry point. Orchestrates per-message routing and lifecycle management.
- **Inputs**: `sqsEvent` containing one or more SQS records; `context` for AWS Lambda runtime.
- **Side effects**:
  - Initializes `AmopLambdaContext` via `BaseAmopFunctionHandler()`.
  - Loads environment variables with `TryGetAllEnvironmentVariables()`.
  - Creates repositories via `InitializeRepositories()`.
  - For each SQS record: logs, parses attributes via `GetMessageValues()`, and dispatches to the appropriate flow.
  - Ensures cleanup via `CleanUp()`.
- **Errors**: Catches and logs exceptions per-record; failed records may be retried by SQS/Lambda according to the queue’s redrive policy.
- **Pseudocode**:
```csharp
public async Task FunctionHandler(SQSEvent sqsEvent, ILambdaContext context)
{
    var amopContext = BaseAmopFunctionHandler(context);
    var config = TryGetAllEnvironmentVariables(amopContext.Logger);

    using var scope = InitializeRepositories(config.CentralDbConnectionString);
    foreach (var record in sqsEvent.Records)
    {
        try
        {
            var values = GetMessageValues(record.MessageAttributes);
            if (values.ServiceProviderId <= 0)
            {
                await InitializeProcessForPendingPages(amopContext, scope.PondRepository, config);
            }
            else
            {
                await ProcessPageByServiceProviderId(amopContext, scope.PondRepository, values, config);
            }
        }
        catch (Exception ex)
        {
            amopContext.Logger.Error(ex, "Failed processing SQS record");
            // Allow SQS retry / DLQ behavior
            throw;
        }
    }
    CleanUp(amopContext);
}
```

#### TryGetAllEnvironmentVariables()
- **Purpose**: Read and validate required environment variables.
- **Returns**: Strongly-typed configuration model with `ProcessStagedDeviceStatusQueueUrl`, `PageSize`, database connection strings, etc.
- **Validations**:
  - Throw or log error if `POND_PROCESS_STAGED_DEVICE_STATUS_QUEUE_URL` is missing in contexts where it’s required (initialization flow).
  - Parse `PAGE_SIZE` or fallback to default.

#### InitializeRepositories(string centralDbConnectionString)
- **Purpose**: Create repository instances for database access.
- **Returns**: Object with initialized `PondRepository`, `ServiceProviderRepository`, and possibly an `EnvironmentRepository`.
- **Notes**: Repositories should share a common SQL transient retry policy (via `RetryPolicyHelper`).

#### GetMessageValues(IDictionary<string, MessageAttributeValue> attrs)
- **Purpose**: Extract `ServiceProviderId`, `PageNumber`, and `IsSuccessful` from SQS message attributes.
- **Returns**: `SqsValues` with typed fields and defaults.
- **Validation**: Missing or non-parsable values are coerced to defaults; logging is performed for invalid inputs.

---

### Initialization Flow Methods

#### InitializeProcessForPendingPages(AmopLambdaContext context, PondRepository pondRepository, Config config)
- **Purpose**: Seed processing by discovering pending pages and enqueuing a message per page.
- **Process**:
  1. Calls `DiscoverPendingPagesToProcess(pondRepository, config.PageSize)`.
  2. For each `(ServiceProviderId, PageNumber)` pending page, calls `InitProcessStagedDeviceStatusPages(...)` to enqueue a message to `ProcessStagedDeviceStatusQueueURL`.
- **Idempotency**: Message enqueue is idempotent at the page key granularity `(ServiceProviderId, PageNumber)`; duplicates are safe because processing is designed to be idempotent.
- **Observability**: Logs pages discovered and messages published; emits counters for fan-out.

#### DiscoverPendingPagesToProcess(PondRepository pondRepository, int pageSize)
- **Purpose**: Query a tracking table to find pages that require processing (unprocessed or failed).
- **Typical sources**:
  - Stored procedure or view such as `POND_GET_DEVICE_STATUS_PAGE_TO_PROCESS` (or re-use of an `..._INVENTORIES_PAGE_TO_PROCESS` pattern with a distinct DeviceStatus step/state).
- **Output**: Collection of `(ServiceProviderId, PageNumber)` pairs.
- **Notes**: May consider sharding or prioritization; page size can influence how many results are returned per discovery call.

#### InitProcessStagedDeviceStatusPages(AmopLambdaContext context, long serviceProviderId, int pageNumber, string queueUrl)
- **Purpose**: Publish an SQS message instructing the Lambda to process a specific page.
- **Message attributes**:
  - `SERVICE_PROVIDER_ID` = `serviceProviderId`
  - `PAGE_NUMBER` = `pageNumber`
- **Behavior**: Uses `SqsService` to send message with the required attributes; may include deduplication/grouping if FIFO is used.
- **Error handling**: Retries on transient SQS errors; logs failures for monitoring.

---

### Processing Flow Methods

#### ProcessPageByServiceProviderId(AmopLambdaContext context, PondRepository pondRepository, SqsValues sqsValues, Config config)
- **Purpose**: Execute the end-to-end processing for a single `(ServiceProviderId, PageNumber)` page.
- **Process**:
  1. `FetchStagedDeviceStatusBatch(...)`
  2. `TransformAndValidate(...)`
  3. `MergeDeviceStatusFromStaging(...)`
  4. `UpdatePageStatusAndCheckProgress(...)`
  5. Optionally publish a completion message/metric when all pages complete.
- **Error handling**:
  - Any error in steps 1–3 results in updating the page status to failed with error details, then rethrowing to leverage SQS retry/DLQ.
  - All DB operations use a SQL transient retry policy.
- **Idempotency**:
  - Merge operations should be upserts with natural keys.
  - Page status updates should tolerate retries (e.g., mark success repeatedly without harmful effects).

#### FetchStagedDeviceStatusBatch(PondRepository pondRepository, long serviceProviderId, int pageNumber, int pageSize)
- **Purpose**: Retrieve staged device-status records for the specified page.
- **Source table**: `PondDeviceStatusStaging` (or equivalent staging table).
- **Output**: List of canonical DTOs for processing.
- **Notes**: Should filter by `ServiceProviderId` and page window; supports large data via paging.

#### TransformAndValidate(IEnumerable<StagedDeviceStatusRow> rows)
- **Purpose**: Convert raw staged rows into a canonical in-memory schema and validate required fields.
- **Validations** (examples):
  - Required fields: DeviceId, ServiceProviderId, Status, StatusTimestamp.
  - Data type checks and normalization (e.g., timestamps to UTC, enum mapping).
  - Deduplication within a page by business key.
- **Output**: Validated list ready for merge; collect and report validation errors.
- **Failure semantics**: If all rows are invalid, mark page as failed; if partially invalid, either drop invalid rows with warnings or fail the page based on policy.

#### MergeDeviceStatusFromStaging(PondRepository pondRepository, IEnumerable<DeviceStatusDto> validRows)
- **Purpose**: Upsert device-status rows into final AMOP 2.0 tables using stored procedures.
- **Typical procedures**:
  - `UPDATE_POND_DEVICE_STATUS_FROM_STAGING`
  - `UPSERT_POND_DEVICE_STATUS`
- **Behavior**:
  - Execute within `RetryPolicyHelper` for transient SQL errors.
  - Ensure atomicity at a batch or row level as supported by the procedure.
  - Maintain idempotency by using natural keys and merge semantics in SQL.
- **Outputs**: Row counts merged/updated, error details if any.

#### UpdatePageStatusAndCheckProgress(PondRepository pondRepository, long serviceProviderId, int pageNumber, PageOutcome outcome, PageMetrics metrics)
- **Purpose**: Update the tracking table row for the processed page and check overall run completion.
- **Tracking table**: `POND_GET_DEVICE_STATUS_PAGE_TO_PROCESS` (or equivalent `..._PAGE_TO_PROCESS`).
- **Updates**:
  - Status: success/failure
  - Counts: read, merged, skipped, failed
  - Error info: messages/exceptions for diagnostics
  - Timestamps: started/completed
- **Completion**:
  - After updating the page, query to determine if all pages for the given run/service provider are complete.
  - If complete, optionally set a final completion flag and/or publish a completion SQS message/metric.

---

### Orchestration Helper

#### SyncDeviceStatus(AmopLambdaContext context, PondRepository pondRepository, SqsValues values, Config config)
- **Purpose**: Coordinate the standard processing sequence with retries and consistent logging.
- **Steps**:
  1. `FetchStagedDeviceStatusBatch`
  2. `TransformAndValidate`
  3. `MergeDeviceStatusFromStaging`
  4. `UpdatePageStatusAndCheckProgress`
- **Policy**: Applies `RetryPolicyHelper` around database calls; logs structured events for each phase; propagates upstream `IsSuccessful` for telemetry if present.

---

### Key Dependencies and Integrations
- **AwsFunctionBase**: Provides base Lambda lifecycle, logging, configuration, and `CleanUp()`.
- **PondRepository**: Encapsulates DB CRUD operations against staging and tracking tables, and invokes stored procedures.
  - Example methods: `GetDeviceStatusStagingPage`, `UpdateDeviceStatusPageStatusAndCheckSyncProgress`, `GetDeviceStatusPagesToProcess`.
- **ServiceProviderRepository**: Optional dependency to enrich service-provider metadata if needed.
- **EnvironmentRepository**: Access to environment variables and config models.
- **SqsService**: Publishes SQS messages, used in initialization and optional completion signaling.
- **RetryPolicyHelper**: Applies SQL transient retry (e.g., exponential backoff) to DB calls.

---

### Data Flow Summary
1. **Input**: SQS message contains `SERVICE_PROVIDER_ID` and `PAGE_NUMBER`.
2. **Stage Read**: Rows are read from `PondDeviceStatusStaging` for that page.
3. **Transform**: Normalize and validate into canonical schema.
4. **Merge**: Stored procedures upsert into final device-status tables.
5. **Advance**: Tracking table updated; completion optionally signaled.

---

### Error Handling, Retries, and Idempotency
- **SQS/Lambda Retries**: On unhandled exceptions, message is retried based on SQS redrive policy; DLQ captures poison messages.
- **SQL Retries**: All DB work wrapped with `RetryPolicyHelper` to mitigate transient errors.
- **Idempotency**:
  - Merge procedures rely on natural keys to safely upsert on retries.
  - Page status updates must handle repeated success marks without side effects.
  - Initialization fan-out can be repeated; duplicates should not double-process due to idempotent page processing.

---

### Observability
- **Logging**: Structured logs for each step, including counts, page keys, and durations.
- **Metrics**:
  - Count of pages discovered and enqueued in initialization.
  - Rows read, merged, skipped, failed per page.
  - Page success/failure counters.
  - Optional completion metric when all pages complete.
- **Tracing**: If enabled, correlate by `(ServiceProviderId, PageNumber)` and SQS message IDs.

---

### Security and Configuration Notes
- Store connection strings in secure configuration (e.g., AWS Secrets Manager or encrypted env vars).
- Least-privilege IAM:
  - Lambda → SQS: `SendMessage` to processing queue; if FIFO, include deduplication.
  - Lambda → RDS/SQL: Network and credentials for DB access.
- Validate and sanitize inputs from staging to protect downstream systems.

---

### Appendix: Representative Pseudocode

```csharp
// Initialization mode
private async Task InitializeProcessForPendingPages(AmopLambdaContext ctx, PondRepository repo, Config cfg)
{
    var pages = await repo.GetDeviceStatusPagesToProcess(cfg.PageSize);
    foreach (var page in pages)
    {
        await InitProcessStagedDeviceStatusPages(ctx, page.ServiceProviderId, page.PageNumber, cfg.ProcessStagedDeviceStatusQueueUrl);
    }
}

// Processing mode
private async Task ProcessPageByServiceProviderId(AmopLambdaContext ctx, PondRepository repo, SqsValues values, Config cfg)
{
    try
    {
        var staged = await FetchStagedDeviceStatusBatch(repo, values.ServiceProviderId, values.PageNumber, cfg.PageSize);
        var valid = TransformAndValidate(staged);
        await MergeDeviceStatusFromStaging(repo, valid);
        await UpdatePageStatusAndCheckProgress(repo, values.ServiceProviderId, values.PageNumber, PageOutcome.Success, MakeMetrics(valid, staged));
    }
    catch (Exception ex)
    {
        await UpdatePageStatusAndCheckProgress(repo, values.ServiceProviderId, values.PageNumber, PageOutcome.Failed, MakeErrorMetrics(ex));
        throw;
    }
}
```

---

### Glossary
- **Page**: A slice of staged data for a specific `ServiceProviderId`, sized by `PAGE_SIZE`.
- **Run/Completion**: A unit of work comprising all pages for a given load; computed when all tracked pages reach a terminal status.
- **Staging**: Temporary landing table(s) used before merging into final, curated tables.

---

### Quick Start Checklist
- Ensure `POND_PROCESS_STAGED_DEVICE_STATUS_QUEUE_URL` is configured.
- Optionally set `PAGE_SIZE`; otherwise, default is used.
- Confirm DB procedures exist: `UPDATE_POND_DEVICE_STATUS_FROM_STAGING`, `UPSERT_POND_DEVICE_STATUS`.
- Verify tracking table/view for pages to process is available.
- Confirm IAM permissions for SQS and DB access.

---

If you need this tailored to concrete class and method names from your codebase (with file references), provide the repository paths and I'll enrich this document with exact signatures and examples.