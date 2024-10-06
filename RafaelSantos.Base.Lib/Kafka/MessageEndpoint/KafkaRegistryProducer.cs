namespace RafaelSantos.Base.Lib.Kafka.MessageEndpoint
{
    public class KafkaRegistryProducer : IEndpointsConfigurator
    {
        private readonly KafkaConfig _brokerConfig;
        public KafkaRegistryProducer(KafkaConfig brokerConfig)
        {
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
                //_logger.Error("Error on Broker Configs", new Exception("Configs not available for Kafka Broker"));
            }
        }
        private void AddKafkaEndpoints(IEndpointsConfigurationBuilder builder)
        {
            Type[] List = BrokerExtension.GetAllMessageTypesFromAssemblies();

            if (string.IsNullOrWhiteSpace(_brokerConfig.Producers))
            {
                foreach (Type type in List)
                {
                    string QueueName = "";
                    string TopicName = "";
                    int partition = 1;
                    try
                    {
                        QueueName = (string?)type.GetProperty("QueueName")?.GetValue(type.GetDefaultValue()) ?? string.Empty;
                        TopicName = (string?)type.GetProperty("TopicName")?.GetValue(type.GetDefaultValue()) ?? string.Empty;
                        partition = (int?)type.GetProperty("Partitions")?.GetValue(type.GetDefaultValue()) ?? 1;
                        Console.WriteLine($"Add Kafka Producer -> QueueName : {QueueName} | TopicName :  {TopicName} | Partition : {partition}");
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
                                         .ProduceTo(TopicName)
                                         .SerializeAsJsonUsingNewtonsoft()
                                         .SerializeUsing((IMessageSerializer)created)
                                         .ValidateMessage(throwException: false)
                                         .DisableMessageValidation()));
                    }
                    catch (Exception ex)
                    {
                        //_logger.Error(QueueName, ex);
                    }
                }
            }
            else
            {
                string QueueName = "";
                string TopicName = "";
                int partition = 1;
                List<string> producers = [.. _brokerConfig.Producers.ToLower().Split(',')];
                foreach (Type type in List)
                {
                    try
                    {

                        QueueName = (string?)type.GetProperty("QueueName")?.GetValue(type.GetDefaultValue()) ?? string.Empty;
                        TopicName = (string?)type.GetProperty("TopicName")?.GetValue(type.GetDefaultValue()) ?? string.Empty;
                        partition = (int?)type.GetProperty("Partitions")?.GetValue(type.GetDefaultValue()) ?? 1;
                        Console.WriteLine($"Add Kafka producer -> QueueName : {QueueName} | TopicName :  {TopicName} | Partition : {partition}");
                        if (producers.Contains((string?)type.GetProperty("TopicName")?.GetValue(type.GetDefaultValue()) ?? string.Empty))
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
                                             .ProduceTo(TopicName)
                                             .SerializeUsing((IMessageSerializer)created)
                                             .ValidateMessage(throwException: false)
                                             .DisableMessageValidation()));

                        }
                    }
                    catch (Exception ex)
                    {
                        //_logger.Error(QueueName, ex);
                    }
                }
            }
        }

    }
}
