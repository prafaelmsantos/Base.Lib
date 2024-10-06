namespace Base.Lib.Kafka.Contracts
{
    public class MessageBase
    {
        [JsonPropertyName("id")]
        public long Id { get; set; }

        [JsonPropertyName("message")]
        public string Message { get; set; } = null!;
    }
}
