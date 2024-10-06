namespace RafaelSantos.Base.Lib.Kafka.ServiceRegistration
{
    public static class ServiceCollectionExtension
    {
        public static IServiceCollection AddKafkaServices(this IServiceCollection services)
        {
            return services.AddKafkaServices(services.BuildServiceProvider().GetRequiredService<IConfiguration>());
        }
        public static IServiceCollection AddKafkaServices(this IServiceCollection services, IConfiguration configuration)
        {
            string topics = Environment.GetEnvironmentVariable("KAFKA_TOPICS") ?? configuration?.GetValue<string>("Kafka:Topics") ?? string.Empty;
            string producers = Environment.GetEnvironmentVariable("KAFKA_PRODUCERS") ?? configuration?.GetValue<string>("Kafka:Producers") ?? string.Empty;
            string consumers = Environment.GetEnvironmentVariable("KAFKA_CONSUMERS") ?? configuration?.GetValue<string>("Kafka:Consumers") ?? string.Empty;

            bool validEnable = bool.TryParse(Environment.GetEnvironmentVariable("KAFKA_ENABLE"), out bool enableValue);
            bool enable = validEnable ? enableValue : configuration?.GetValue<bool>("Kafka:Enable") ?? false;

            string name = Environment.GetEnvironmentVariable("KAFKA_NAME") ?? configuration?.GetValue<string>("Kafka:Name") ?? string.Empty;
            string password = Environment.GetEnvironmentVariable("KAFKA_PASSWORD") ?? configuration?.GetValue<string>("Kafka:Password") ?? string.Empty;

            bool validPort = int.TryParse(Environment.GetEnvironmentVariable("KAFKA_PORT"), out int portValue);
            int port = validPort ? portValue : configuration?.GetValue<int>("Kafka:Port") ?? 0;

            string server = Environment.GetEnvironmentVariable("KAFKA_SERVER") ?? configuration?.GetValue<string>("Kafka:Server") ?? string.Empty;
            string username = Environment.GetEnvironmentVariable("KAFKA_USERNAME") ?? configuration?.GetValue<string>("Kafka:Username") ?? string.Empty;

            bool validuseSSL_TLS = bool.TryParse(Environment.GetEnvironmentVariable("KAFKA_USESSL_TLS"), out bool useSSL_TLSValue);
            bool useSSL_TLS = validEnable ? enableValue : configuration?.GetValue<bool>("Kafka:UseSSL_TLS") ?? false;

            bool validRetryAttempts = int.TryParse(Environment.GetEnvironmentVariable("KAFKA_RETRY_ATTEMPTS"), out int retryAttemptsValue);
            int retryAttempts = validPort ? retryAttemptsValue : configuration?.GetValue<int>("Kafka:RetryAttempts") ?? 5;

            bool validPartitions = int.TryParse(Environment.GetEnvironmentVariable("KAFKA_PARTITIONS"), out int partitionsValue);
            int partitions = validPort ? partitionsValue : configuration?.GetValue<int>("Kafka:Partitions") ?? 5;

            string groupId = Environment.GetEnvironmentVariable("KAFKA_GROUP_ID") ?? configuration?.GetValue<string>("Kafka:GroupId") ?? string.Empty;

            services.AddSilverback();
            services.AddScoped<IKafkaProducer, Services.KafkaProducer>();

            KafkaConfig brokerConfig = new(topics, producers, consumers, enable, name, useSSL_TLS, server, port, username, password, retryAttempts, partitions, groupId);
            services.AddSingleton(brokerConfig);

            services.ConfigureSilverback().WithConnectionToMessageBroker(options => options
                .AddKafka())
                .AddEndpointsConfigurator<KafkaRegistryConsumer>()
                .AddEndpointsConfigurator<KafkaRegistryProducer>();

            return services;
        }
    }
}
