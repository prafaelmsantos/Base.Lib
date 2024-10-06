namespace Base.Lib.Kafka.Models
{
    public class MessageBase
    {
        [JsonPropertyName("id")]
        public long Id { get; set; }

        [JsonPropertyName("message")]
        public string Message { get; set; } = null!;
    }
}
