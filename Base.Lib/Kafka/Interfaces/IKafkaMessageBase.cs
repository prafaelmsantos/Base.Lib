namespace Base.Lib.Kafka.Interfaces
{
    public interface IKafkaMessageBase
    {
        public static string QueueName { get; } = null!;
        public static string TopicName { get; } = null!;
        public static int BatchProcessing { get; }
        public static int BatchProcessingInterval { get; }
        public static int Partitions { get; }
    }
}
