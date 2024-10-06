namespace Base.Lib.Kafka.Interfaces
{
    public interface IKafkaConsumer<TEntity>
        where TEntity : IBaseBrokerMessage
    {
        Task OnBatchReceivedAsync(IAsyncEnumerable<TEntity> message);
    }
}