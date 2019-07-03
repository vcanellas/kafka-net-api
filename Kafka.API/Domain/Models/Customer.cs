namespace Kafka.API.Domain.Models
{
    public class Customer
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public Status Status { get; set; }
    }

    public enum Status
    {
        INSERTED = 1,
        APPROVED = 2,
        REMOVED = 3 
    }
}