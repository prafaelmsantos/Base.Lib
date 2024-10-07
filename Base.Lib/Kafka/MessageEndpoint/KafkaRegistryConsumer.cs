namespace Base.Lib.Kafka.MessageEndpoint
{
    public class KafkaRegistryConsumer : IEndpointsConfigurator
    {
        #region Private variables
        private readonly ILogger<KafkaRegistryConsumer> _logger;
        private readonly KafkaConfig _brokerConfig;
        private readonly IAdminClient _adminClient;
        #endregion

        #region Constructors

        public KafkaRegistryConsumer(ILogger<KafkaRegistryConsumer> logger, KafkaConfig brokerConfig)
        {
            _logger = logger;
            _brokerConfig = brokerConfig;

            List<KeyValuePair<string, string>> clientConfig =
            [
                new("bootstrap.servers", $"{_brokerConfig.Server}:{_brokerConfig.Port}")
            ];

            _adminClient = new AdminClientBuilder(clientConfig).Build();
        }
        #endregion

        #region Public methods
        public void Configure(IEndpointsConfigurationBuilder builder)
        {
            if (_brokerConfig.Enable)
            {
                AddKafkaEndpoints(builder);
            }
            else
            {
                Exception ex = new("Configs not available for Kafka Broker");
                _logger.LogError(ex.Message, ex);
            }
        }
        #endregion

        #region Private methods
        private void AddKafkaEndpoints(IEndpointsConfigurationBuilder builder)
        {
            Type[] existingContracts = BrokerExtension.GetAllMessageTypesFromAssemblies();

            foreach (Type contract in existingContracts)
            {
                try
                {
                    var consumers = GetRegistredConsumers();
                    if (consumers.Contains(contract.Name))
                    {
                        string topicName = (string?)contract.GetProperty("TopicName")?.GetValue(contract.GetDefaultValue()) ?? string.Empty;
                        string queueName = (string?)contract.GetProperty("QueueName")?.GetValue(contract.GetDefaultValue()) ?? string.Empty;
                        int batchSize = (int?)contract.GetProperty("BatchProcessing")?.GetValue(contract.GetDefaultValue()) ?? 100;
                        int batchInterval = (int?)contract.GetProperty("BatchProcessingInterval")?.GetValue(contract.GetDefaultValue()) ?? 5;
                        string groupId = $"{queueName}_{(string?)contract.GetProperty("GroupId")?.GetValue(contract.GetDefaultValue()) ?? string.Empty}";

                        CreateTopic(topicName, _brokerConfig.Partitions);

                        Type genericClass = typeof(JsonMessageSerializer<>);
                        Type constructedClass = genericClass.MakeGenericType(contract);
                        object created = Activator.CreateInstance(constructedClass)!;

                        builder
                            .AddKafkaEndpoints(endpoints => endpoints
                                        .Configure(kafkaClientConfig =>
                                        {
                                            kafkaClientConfig.BootstrapServers = $"{_brokerConfig.Server}:{_brokerConfig.Port}";
                                        })
                                        .AddInbound(endpoint => endpoint
                                                .ConsumeFrom(topicName)
                                                .DeserializeUsing((IMessageSerializer)created!)
                                                .ValidateMessage(throwException: false)
                                                .DisableMessageValidation()
                                                .Configure(
                                                    config =>
                                                    {
                                                        config.AllowAutoCreateTopics = true;
                                                        config.GroupId = groupId;
                                                        config.AutoOffsetReset = AutoOffsetReset.Earliest;
                                                    })
                                                .EnableBatchProcessing(batchSize, TimeSpan.FromSeconds(batchInterval))
                                                .OnError(policy => policy.Retry(_brokerConfig.RetryAttempts))));
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex.Message, ex);
                }
            }
        }

        private void CreateTopic(string topicName, int partitions)
        {
            try
            {
                List<TopicSpecification> topicSpecifications =
                [
                    new()
                    {
                        Name = topicName,
                        ReplicationFactor = 1,
                        NumPartitions = partitions
                    },
                ];

                _adminClient.CreateTopicsAsync(topicSpecifications).Wait();
            }
            catch (AggregateException ex)
            {
                _logger.LogWarning(ex.Message, ex);
            }
        }

        private List<string> GetRegistredConsumers()
        {
            string consumers = _brokerConfig.Consumers;
            return string.IsNullOrWhiteSpace(consumers) ? [] : [.. consumers.Trim().Split(",")];
        }
        #endregion
    }
}

