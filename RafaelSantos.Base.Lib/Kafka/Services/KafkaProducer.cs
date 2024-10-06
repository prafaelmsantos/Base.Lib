namespace RafaelSantos.Base.Lib.Kafka.Services
{
    public class KafkaProducer : IKafkaProducer
    {
        private readonly IPublisher _publisher;
        private readonly IBroker _broker;

        public KafkaProducer(IPublisher publisher, IBroker broker)
        {
            _publisher = publisher;
            _broker = broker;
        }

        public async Task<bool> ProduceMessageAsync<T>(T message)
        {
            try
            {
                if (_broker.IsConnected)
                {
                    await _publisher.PublishAsync(message!);

                    //_logger.Information($"Produced {message}");
                }
                else
                {
                    Exception ex = new("Broker is not connected.");
                    //_logger.Error("Broker is not connected", ex);
                    throw ex;
                }

                return true;
            }
            catch (Exception ex)
            {
                //_logger.Error($"Failed to produce {message}", ex);
                throw;
            }
        }
    }
}
