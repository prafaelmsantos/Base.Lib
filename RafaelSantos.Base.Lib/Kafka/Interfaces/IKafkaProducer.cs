namespace RafaelSantos.Base.Lib.Kafka.Interfaces
{
    public interface IKafkaProducer
    {
        Task<bool> ProduceMessageAsync<T>(T message);
    }
}