using Altaworx.AWS.Core;
using Altaworx.AWS.Core.Models;
using Amazon.Lambda.Core;
using Amazon.Lambda.SQSEvents;
using Amazon.SQS.Model;
using Amazon.SQS;
using Amop.Core.Constants;
using Amop.Core.Helpers.Pond;
using Amop.Core.Models;
using Amop.Core.Repositories.Environment;
using Amop.Core.Repositories.Pond;
using Amazon;
using Amop.Core.Helpers;
using Amop.Core.Models.Pond;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace AltaworxPondProcessStagedDeviceStatus
{
    public class Function : AwsFunctionBase
    {
        private string GetRatePlansQueueURL = string.Empty;
        private readonly EnvironmentRepository environmentRepo = new EnvironmentRepository();
        private PondRepository pondRepository;

        public async Task FunctionHandler(SQSEvent sqsEvent, ILambdaContext context)
        {
            AmopLambdaContext? lambdaContext = null;
            try
            {
                lambdaContext = BaseAmopFunctionHandler(context);
                LogInfo(lambdaContext, CommonConstants.SUB, LogCommonStrings.POND_START_PROCESSING_STAGED_DEVICE_STATUS);

                ArgumentNullException.ThrowIfNull(lambdaContext);

                InitializeRepositories(lambdaContext);

                TryGetAllEnvironmentVariable(lambdaContext);

                await ProcessEventAsync(lambdaContext, sqsEvent);
            }
            catch (Exception ex)
            {
                if (lambdaContext == null)
                {
                    context.Logger.Log(CommonConstants.EXCEPTION, ex.Message + " " + ex.StackTrace);
                }
                else
                {
                    LogInfo(lambdaContext, CommonConstants.EXCEPTION, ex.Message + " " + ex.StackTrace);
                }
            }

            base.CleanUp(lambdaContext);
        }

        protected async Task ProcessEventAsync(AmopLambdaContext context, SQSEvent sqsEvent)
        {
            LogInfo(context, CommonConstants.SUB);
            if (sqsEvent?.Records != null)
            {
                var processedRecordCount = sqsEvent.Records.Count;
                LogInfo(context, CommonConstants.INFO, string.Format(LogCommonStrings.BEGINNING_PROCESS, processedRecordCount));
                foreach (var record in sqsEvent.Records)
                {
                    LogInfo(context, CommonConstants.INFO, $"MessageId: {record.MessageId}");
                    await ProcessCleanUpDeviceSync(context);
                }
            }
        }

        private async Task ProcessCleanUpDeviceSync(AmopLambdaContext context)
        {
            // Check remaining device to sync status
            var countRemaining = pondRepository.CountRemainingDeviceStatusToProcess(ParameterizedLog(context));
            if (countRemaining == 0)
            {
                var serviceProviderIdList = pondRepository.GetDeviceStatusServiceProviderIds(ParameterizedLog(context));
                SyncStatusToPondDevice(context, serviceProviderIdList);
                foreach (var serviceProviderId in serviceProviderIdList)
                {
                    await SendMessageToPondGetRatePlanQueue(context, GetRatePlansQueueURL);
                }
                pondRepository.TruncatePondDeviceStatusICCIDsToProcessTable(ParameterizedLog(context));
            }
            else
            {
                LogInfo(context, CommonConstants.INFO, string.Format(LogCommonStrings.REMAINING_RECORD_COUNT, countRemaining));
            }
        }

        protected void TryGetAllEnvironmentVariable(AmopLambdaContext lambdaContext)
        {
            GetRatePlansQueueURL = GetStringValueFromEnvironmentVariable(lambdaContext.Context, environmentRepo,
                PondHelper.CommonString.POND_RATE_PLANS_QUEUE_URL_VARIABLE_KEY);
        }

        protected virtual void InitializeRepositories(AmopLambdaContext lambdaContext)
        {
            pondRepository = new PondRepository(lambdaContext.CentralDbConnectionString);
        }

        protected void SyncStatusToPondDevice(KeySysLambdaContext lambdaContext, List<int> serviceProviderIdList)
        {
            LogInfo(lambdaContext, CommonConstants.INFO, LogCommonStrings.STARTING_SYNC_POND_DEVICE_STATUS);

            foreach (var serviceProviderId in serviceProviderIdList)
            {
                var pondAuthentication = pondRepository.GetPondAuthentication(ParameterizedLog(lambdaContext), lambdaContext.Base64Service, serviceProviderId);
                if (pondAuthentication == null)
                {
                    LogInfo(lambdaContext, CommonConstants.WARNING, string.Format(LogCommonStrings.SERVICE_PROVIDER_NO_AUTH_INFO, serviceProviderId));
                    continue;
                }

                pondRepository.UpdateDeviceStatus(ParameterizedLog(lambdaContext), serviceProviderId, pondAuthentication.BillPeriodEndDay, pondAuthentication.BillPeriodEndHour);
            }

            LogInfo(lambdaContext, CommonConstants.INFO, LogCommonStrings.POND_DEVICE_STATUS_SYNC_ENDED);
        }

        private async Task SendMessageToPondGetRatePlanQueue(KeySysLambdaContext context, string queueURL)
        {
            LogInfo(context, CommonConstants.SUB, $"({queueURL})");
            var awsCredentials = AwsCredentials(context);
            using (var client = new AmazonSQSClient(awsCredentials, RegionEndpoint.USEast1))
            {
                LogInfo(context, CommonConstants.INFO, string.Format(LogCommonStrings.SENDING_MESSAGE_TO_URL, queueURL));

                var request = new SendMessageRequest
                {
                    MessageAttributes = new Dictionary<string, MessageAttributeValue>
                    {
                        { SQSMessageKeyConstant.SYNC_ACTION, new MessageAttributeValue { DataType = nameof(String), StringValue = ((int)PondSyncAction.SyncFromAPIToStaging).ToString()}},
                    },
                    MessageBody = LogCommonStrings.END_PROCESS_GET_DEVICES,
                    QueueUrl = queueURL
                };

                LogInfo(context, CommonConstants.INFO, LogCommonStrings.SEND_MESSAGE_REQUEST_READY);
                LogInfo(context, CommonConstants.INFO, $"{CommonConstants.MESSAGE_BODY}: {request.MessageBody}");

                var response = await client.SendMessageAsync(request);
                LogInfo(context, CommonConstants.INFO, response.HttpStatusCode);
            }
        }
    }
}