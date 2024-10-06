namespace RafaelSantos.Base.Lib.Kafka.MessageEndpoint
{
    public class KafkaRegistryConsumer : IEndpointsConfigurator
    {
        private readonly KafkaConfig _brokerConfig;
        private readonly List<string> _consumers;
        private readonly IAdminClient _adminClient;

        public KafkaRegistryConsumer(KafkaConfig brokerConfig)
        {
            _brokerConfig = brokerConfig;
            _consumers = GetRegistredConsumers();

            List<KeyValuePair<string, string>> clientConfig =
            [
                new("bootstrap.servers", $"{_brokerConfig.Server}:{_brokerConfig.Port}")
            ];

            _adminClient = new AdminClientBuilder(clientConfig).Build();
        }

        public void Configure(IEndpointsConfigurationBuilder builder)
        {
            if (_brokerConfig.Enable)
            {
                AddKafkaEndpoints(builder);
            }
            else
            {
                //_logger.Error("Error on Broker Configs", new Exception("Configs not available for Kafka Broker"));
            }
        }

        private void AddKafkaEndpoints(IEndpointsConfigurationBuilder builder)
        {
            Type[] existingContracts = BrokerExtension.GetAllMessageTypesFromAssemblies();

            foreach (Type contract in existingContracts)
            {
                try
                {
                    if (_consumers.Contains(contract.Name))
                    {
                        string topicName = (string?)contract.GetProperty("TopicName")?.GetValue(contract.GetDefaultValue()) ?? string.Empty;
                        string queueName = (string?)contract.GetProperty("QueueName")?.GetValue(contract.GetDefaultValue()) ?? string.Empty;
                        int batchSize = (int?)contract.GetProperty("BatchProcessing")?.GetValue(contract.GetDefaultValue()) ?? 100;
                        int batchInterval = (int?)contract.GetProperty("BatchProcessingInterval")?.GetValue(contract.GetDefaultValue()) ?? 5;
                        string consumerGroup = $"{queueName}_{_brokerConfig.GroupId}";

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
                                                        config.GroupId = consumerGroup;
                                                        config.AutoOffsetReset = AutoOffsetReset.Earliest;
                                                    })
                                                .EnableBatchProcessing(batchSize, TimeSpan.FromSeconds(batchInterval))
                                                .OnError(policy => policy.Retry(_brokerConfig.RetryAttempts))));
                    }
                }
                catch (Exception ex)
                {
                    // _logger.Error(ex.Message, ex);
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
            catch (AggregateException e)
            {
                //_logger.Warning(e.Message);
            }

        }

        private List<string> GetRegistredConsumers()
        {
            string consumers = _brokerConfig.Consumers;
            return string.IsNullOrWhiteSpace(consumers) ? [] : [.. consumers.Trim().Split(",")];
        }
    }
}

