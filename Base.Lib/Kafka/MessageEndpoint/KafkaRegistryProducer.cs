namespace Base.Lib.Kafka.MessageEndpoint
{
    public class KafkaRegistryProducer : IEndpointsConfigurator
    {
        #region Private variables
        private readonly ILogger<KafkaRegistryProducer> _logger;
        private readonly KafkaConfig _brokerConfig;
        #endregion

        #region Constructors

        public KafkaRegistryProducer(ILogger<KafkaRegistryProducer> logger, KafkaConfig brokerConfig)
        {
            _logger = logger;
            _brokerConfig = brokerConfig;
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

            foreach (Type type in existingContracts)
            {
                try
                {
                    var producers = GetRegistredProducers();
                    if (producers.Contains(type.Name))
                    {
                        string queueName = (string?)type.GetProperty("QueueName", BindingFlags.Public | BindingFlags.Static)?.GetValue(type.GetDefaultValue()) ?? string.Empty;
                        string topicName = (string?)type.GetProperty("TopicName", BindingFlags.Public | BindingFlags.Static)?.GetValue(type.GetDefaultValue()) ?? string.Empty;
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
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex.Message, ex);
                }
            }
        }

        private List<string> GetRegistredProducers()
        {
            string producers = _brokerConfig.Producers;
            return string.IsNullOrWhiteSpace(producers) ? [] : [.. producers.Trim().Split(",")];
        }
        #endregion
    }
}
