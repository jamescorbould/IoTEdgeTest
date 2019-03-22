using System.Collections.Generic;
using Newtonsoft.Json;

namespace AzureIoTEdgeModule1
{
    using System;
    using System.IO;
    using System.Runtime.InteropServices;
    using System.Runtime.Loader;
    using System.Security.Cryptography.X509Certificates;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Devices.Client;

    class Program
    {
        static int counter;
        const int temperatureThresholdLow = 25;
        const int temperatureThresholdHigh = 28;

        static void Main()
        {
            Init().Wait();

            // Wait until the app unloads or is cancelled
            var cts = new CancellationTokenSource();
            AssemblyLoadContext.Default.Unloading += (ctx) => cts.Cancel();
            Console.CancelKeyPress += (sender, cpe) => cts.Cancel();
            WhenCancelled(cts.Token).Wait();
        }

        /// <summary>
        /// Handles cleanup operations when app is cancelled or unloads
        /// </summary>
        public static Task WhenCancelled(CancellationToken cancellationToken)
        {
            var tcs = new TaskCompletionSource<bool>();
            cancellationToken.Register(s => ((TaskCompletionSource<bool>) s).SetResult(true), tcs);
            return tcs.Task;
        }

        /// <summary>
        /// Initializes the ModuleClient and sets up the callback to receive
        /// messages containing temperature information
        /// </summary>
        static async Task Init()
        {
            AmqpTransportSettings amqpSetting = new AmqpTransportSettings(TransportType.Amqp_Tcp_Only);
            ITransportSettings[] settings = {amqpSetting};

            // Open a connection to the Edge runtime
            ModuleClient ioTHubModuleClient =
                await ModuleClient.CreateFromEnvironmentAsync(settings).ConfigureAwait(false);
            await ioTHubModuleClient.OpenAsync().ConfigureAwait(false);
            Console.WriteLine("IoT Hub module client initialized.");

            // Register callback to be called when a message is received by the module
            await ioTHubModuleClient.SetInputMessageHandlerAsync("input1", PipeMessage, ioTHubModuleClient)
                .ConfigureAwait(false);
        }

        /// <summary>
        /// This method is called whenever the module is sent a message from the EdgeHub. 
        /// It just pipe the messages without any change.
        /// It prints all the incoming messages.
        /// </summary>
        static async Task<MessageResponse> PipeMessage(Message message, object userContext)
        {
            int counterValue = Interlocked.Increment(ref counter);

            try
            {
                var moduleClient = userContext as ModuleClient;
                if (moduleClient == null)
                {
                    throw new InvalidOperationException("UserContext doesn't contain " + "expected values");
                }

                byte[] messageBytes = message.GetBytes();
                string messageString = Encoding.UTF8.GetString(messageBytes);
                Console.WriteLine($"Received message: {counterValue}, Body: [{messageString}]");

                if (!string.IsNullOrEmpty(messageString))
                {
                    var messageBody = JsonConvert.DeserializeObject<MessageBody>(messageString);

                    if (messageBody != null && (messageBody.machine.temperature >= temperatureThresholdLow & messageBody.machine.temperature <= temperatureThresholdHigh))
                    {
                        Console.WriteLine($"Machine temperature {messageBody.machine.temperature} between temperature threshold of {temperatureThresholdLow}-{temperatureThresholdHigh}.");

                        Message filteredMessage = new Message(messageBytes);

                        foreach (KeyValuePair<string, string> prop in message.Properties)
                        {
                            filteredMessage.Properties.Add(prop.Key, prop.Value);
                        }
                        
                        filteredMessage.Properties.Add("MessageType", "Alert");

                        // Send upstream to IoT Hub.
                        await moduleClient.SendEventAsync("output1", filteredMessage).ConfigureAwait(false);
                    }
                }

                Console.WriteLine("Send response completed.");

                return MessageResponse.Completed;
            }
            catch (AggregateException ex)
            {
                foreach (Exception exception in ex.InnerExceptions)
                {
                    Console.WriteLine();
                    Console.WriteLine("Error in sample: {0}", exception);
                }

                // Indicate that the message treatment is not completed.
                var moduleClient = (ModuleClient)userContext;
                return MessageResponse.Abandoned;
            }
            catch (Exception ex)
            {
                Console.WriteLine();
                Console.WriteLine("Error in sample: {0}", ex.Message);

                // Indicate that the message treatment is not completed.
                ModuleClient moduleClient = (ModuleClient)userContext;
                return MessageResponse.Abandoned;
            }
        }
    }

    class MessageBody
    {
        public Machine machine { get; set; }
        public Ambient ambient { get; set; }
        public string timeCreated { get; set; }
    }

    class Machine
    {
        public double temperature { get; set; }
        public double pressure { get; set; }
    }

    class Ambient
    {
        public double temperature { get; set; }
        public int humidity { get; set; }
    }
}