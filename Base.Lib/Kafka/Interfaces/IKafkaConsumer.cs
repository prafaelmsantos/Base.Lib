namespace Base.Lib.Kafka.Interfaces
{
    public interface IKafkaConsumer<TEntity>
        where TEntity : IKafkaMessageBase
    {
        Task OnBatchReceivedAsync(IAsyncEnumerable<TEntity> message);
    }
}