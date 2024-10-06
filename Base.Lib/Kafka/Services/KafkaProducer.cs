namespace Base.Lib.Kafka.Services
{
    public class KafkaProducer : IKafkaProducer
    {
        private readonly ILogger<KafkaProducer> _logger;
        private readonly IPublisher _publisher;
        private readonly IBroker _broker;

        public KafkaProducer(ILogger<KafkaProducer> logger, IPublisher publisher, IBroker broker)
        {
            _logger = logger;
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
                    _logger.LogInformation($"Produced {message}");
                }
                else
                {
                    Exception ex = new("Kafka is not connected.");
                    _logger.LogInformation(ex.Message, ex);
                    throw ex;
                }

                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError($"Failed to produce {message}", ex);
                throw;
            }
        }
    }
}
