namespace Base.Lib.Kafka.Models.Config
{
    public class KafkaConfig
    {
        public string Topics { get; set; } = null!;
        public string? Producers { get; set; }
        public string Consumers { get; set; } = null!;
        public bool Enable { get; set; } = false;
        public string Name { get; set; } = null!;
        public bool UseSSL_TLS { get; set; }
        public string Server { get; set; } = null!;
        public int Port { get; set; }
        public string Username { get; set; } = null!;
        public string Password { get; set; } = null!;
        public int RetryAttempts { get; set; }
        public int Partitions { get; set; }
        public string GroupId { get; set; } = null!;

        public KafkaConfig(
            string topics,
            string? producers,
            string consumers,
            bool enable,
            string name,
            bool useSSL_TLS,
            string server,
            int port,
            string username,
            string password,
            int retryAttempts,
            int partitions,
            string groupId)
        {
            Topics = topics;
            Producers = producers;
            Consumers = consumers;
            Enable = enable;
            Name = name;
            UseSSL_TLS = useSSL_TLS;
            Server = server;
            Port = port;
            Username = username;
            Password = password;
            RetryAttempts = retryAttempts;
            Partitions = partitions;
            GroupId = groupId;
        }
    }
}
