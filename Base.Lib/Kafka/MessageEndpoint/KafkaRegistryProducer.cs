namespace Base.Lib.Kafka.MessageEndpoint
{
    public class KafkaRegistryProducer : IEndpointsConfigurator
    {
        private readonly ILogger<KafkaRegistryProducer> _logger;
        private readonly KafkaConfig _brokerConfig;

        public KafkaRegistryProducer(ILogger<KafkaRegistryProducer> logger, KafkaConfig brokerConfig)
        {
            _logger = logger;
            _brokerConfig = brokerConfig;
        }

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
        private void AddKafkaEndpoints(IEndpointsConfigurationBuilder builder)
        {
            Type[] List = BrokerExtension.GetAllMessageTypesFromAssemblies();

            if (string.IsNullOrWhiteSpace(_brokerConfig.Producers))
            {
                foreach (Type type in List)
                {
                    try
                    {
                        string queueName = (string?)type.GetProperty("queueName")?.GetValue(type.GetDefaultValue()) ?? string.Empty;
                        string topicName = (string?)type.GetProperty("topicName")?.GetValue(type.GetDefaultValue()) ?? string.Empty;
                        int partition = (int?)type.GetProperty("Partitions")?.GetValue(type.GetDefaultValue()) ?? 1;
                        Console.WriteLine($"Add Kafka Producer -> queueName : {queueName} | topicName :  {topicName} | Partition : {partition}");
                        Type genericClass = typeof(JsonMessageSerializer<>);
                        Type constructedClass = genericClass.MakeGenericType(type);
                        object created = Activator.CreateInstance(constructedClass)!;
                        builder
                            .AddKafkaEndpoints(
                                endpoints => endpoints
                                                .Configure(
                                                    config =>
                                                    {
                                                        // The bootstrap server address is needed to connect
                                                        config.BootstrapServers = $"{_brokerConfig.Server}:{_brokerConfig.Port}";
                                                    })
                                .AddOutbound(type, endpoint => endpoint
                                         .ProduceTo(topicName)
                                         .SerializeAsJsonUsingNewtonsoft()
                                         .SerializeUsing((IMessageSerializer)created)
                                         .ValidateMessage(throwException: false)
                                         .DisableMessageValidation()));
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex.Message, ex);
                    }
                }
            }
            else
            {
                List<string> producers = [.. _brokerConfig.Producers.ToLower().Split(',')];
                foreach (Type type in List)
                {
                    try
                    {

                        string queueName = (string?)type.GetProperty("queueName")?.GetValue(type.GetDefaultValue()) ?? string.Empty;
                        string topicName = (string?)type.GetProperty("topicName")?.GetValue(type.GetDefaultValue()) ?? string.Empty;
                        int partition = (int?)type.GetProperty("Partitions")?.GetValue(type.GetDefaultValue()) ?? 1;
                        Console.WriteLine($"Add Kafka producer -> queueName : {queueName} | topicName :  {topicName} | Partition : {partition}");
                        if (producers.Contains((string?)type.GetProperty("topicName")?.GetValue(type.GetDefaultValue()) ?? string.Empty))
                        {
                            Type genericClass = typeof(JsonMessageSerializer<>);
                            Type constructedClass = genericClass.MakeGenericType(type);
                            object created = Activator.CreateInstance(constructedClass)!;
                            builder
                                .AddKafkaEndpoints(
                                    endpoints => endpoints
                                                .Configure(
                                                    config =>
                                                    {
                                                        config.AllowAutoCreateTopics = true;
                                                        // The bootstrap server address is needed to connect
                                                        config.BootstrapServers = $"{_brokerConfig.Server}:{_brokerConfig.Port}";
                                                    })
                                    .AddOutbound(type, endpoint => endpoint
                                             .ProduceTo(topicName)
                                             .SerializeUsing((IMessageSerializer)created)
                                             .ValidateMessage(throwException: false)
                                             .DisableMessageValidation()));

                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex.Message, ex);
                    }
                }
            }
        }
    }
}
