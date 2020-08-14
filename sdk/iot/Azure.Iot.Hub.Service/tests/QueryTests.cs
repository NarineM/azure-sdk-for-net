// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System;
using System.Threading.Tasks;
using Azure.Iot.Hub.Service.Models;
using FluentAssertions;
using NUnit.Framework;

namespace Azure.Iot.Hub.Service.Tests
{
    /// <summary>
    /// Test the query API. This API is a query on Twins only.
    /// </summary>
    /// <remarks>
    /// All API calls are wrapped in a try catch block so we can clean up resources regardless of the test outcome.
    /// </remarks>
    public class QueryTests : E2eTestBase
    {
        public QueryTests(bool isAsync)
            : base(isAsync)
        {
        }

        /// <summary>
        /// Test querying all device twins in the IoTHub.
        /// </summary>
        [Test]
        public async Task QueryClient_GetAllDeviceTwinsWithTag()
        {
            string testDeviceId = $"QueryDevice{GetRandom()}";

            DeviceIdentity device = null;
            IotHubServiceClient client = GetClient();

            try
            {
                // Create a device.
                device = (await client.Devices.CreateOrUpdateIdentityAsync(
                    new DeviceIdentity
                    {
                        DeviceId = testDeviceId
                    })).Value;

                // Add a tag to the device twin.
                Response<TwinData> twin = await client.Devices.GetTwinAsync(testDeviceId);
                twin.Value.Tags.Add("QueryDeviceTagKey", "QueryDeviceTagValue");
                await client.Devices.UpdateTwinAsync(twin);

                // Query for device twins with a specific tag.
                AsyncPageable<TwinData> queryResponse = client.Query.QueryAsync("SELECT  * FROM devices WHERE tags.QueryDeviceTagKey = 'QueryDeviceTagValue'");

                int count = 0;
                await foreach (TwinData item in queryResponse)
                {
                    count++;
                }

                count.Should().Be(1);

                // Delete the device
                // Deleting the device happens in the finally block as cleanup.
            }
            finally
            {
                await Cleanup(client, device);
            }
        }

        /// <summary>
        /// Test querying all device twins in the IoTHub.
        /// </summary>
        [Test]
        public async Task QueryClient_GetAllModuleTwinsWithTag()
        {
            string testDeviceId = $"QueryDevice{GetRandom()}";
            string testModuleId = $"QueryDevice{GetRandom()}";

            DeviceIdentity device = null;
            IotHubServiceClient client = GetClient();

            try
            {
                // Create a device to house the module
                device = (await client.Devices.CreateOrUpdateIdentityAsync(
                    new DeviceIdentity
                    {
                        DeviceId = testDeviceId
                    })).Value;

                // Create a module on the device
                Response<ModuleIdentity> createResponse = await client.Modules.CreateOrUpdateIdentityAsync(
                    new ModuleIdentity
                    {
                        DeviceId = testDeviceId,
                        ModuleId = testModuleId
                    }).ConfigureAwait(false);

                // Add a tag to the module twin.
                Response<TwinData> twin = await client.Modules.GetTwinAsync(testDeviceId, testModuleId);
                twin.Value.Tags.Add("QueryModuleTagKey", "QueryModuleTagValue");
                await client.Modules.UpdateTwinAsync(twin);

                // Query for module twins with a specific tag.
                AsyncPageable<TwinData> queryResponse = client.Query.QueryAsync("SELECT  * FROM devices.modules WHERE tags.QueryModuleTagKey = 'QueryModuleTagValue'");

                int count = 0;
                await foreach (TwinData item in queryResponse)
                {
                    count++;
                }

                count.Should().Be(1);

                // Delete the device
                // Deleting the device happens in the finally block as cleanup.
            }
            finally
            {
                await Cleanup(client, device);
            }
        }

        private async Task Cleanup(IotHubServiceClient client, DeviceIdentity device)
        {
            // cleanup
            try
            {
                if (device != null)
                {
                    await client.Devices.DeleteIdentityAsync(device, IfMatchPrecondition.UnconditionalIfMatch).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                Assert.Fail($"Test clean up failed: {ex.Message}");
            }
        }
    }
}
