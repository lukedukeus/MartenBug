using JasperFx;
using JasperFx.Events;
using JasperFx.Events.Projections;
using Marten;
using Marten.Events.Aggregation;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

using NetTopologySuite.Utilities;

using Testcontainers.PostgreSql;

namespace MartenBug
{
    public class Program
    {
        static async Task Main(string[] args)
        {
            var postgreSql = new PostgreSqlBuilder().WithImage("postgres:16").Build();

            await postgreSql.StartAsync();

            var host = Host.CreateDefaultBuilder().ConfigureServices((context, services) =>
            {
                services.AddMarten(options =>
                {
                    options.Connection(postgreSql.GetConnectionString());
                    options.UseSystemTextJsonForSerialization();
                    options.AutoCreateSchemaObjects = AutoCreate.All;

                    options.Events.AddEventType<UserCreated>();

                    options.Schema.For<EventSummary>();
                    options.Projections.Add<EventSummaryProjection>(ProjectionLifecycle.Inline);
                });
            })
           .Build();

            IQuerySession querySession = host.Services.GetRequiredService<IQuerySession>();
            IDocumentStore documentStore = host.Services.GetRequiredService<IDocumentStore>();
            IServiceProvider serviceProvider = host.Services.GetRequiredService<IServiceProvider>();

            var userId = Guid.CreateVersion7();
            UserReference user = new UserReference(userId, "testuser", "secret");

            await using (var session = documentStore.LightweightSession())
            {
                var userCreatedEvent = new UserCreated(user);

                var stream = session.Events.StartStream<ICreatedEvent>(userCreatedEvent.StreamId, userCreatedEvent);

                await session.SaveChangesAsync();
            }

            await using (var session = documentStore.LightweightSession())
            {
                var userLoggedInEvent = new UserLoggedIn(user);

                var stream = session.Events.Append(userLoggedInEvent.StreamId, 2, userLoggedInEvent);

                await session.SaveChangesAsync();
            }

            // Works fine
            var eventSummaries = querySession.Query<EventSummary>().ToList();

            Assert.Equals(eventSummaries.Count, 2);
        }
    }

    public record UserReference(Guid Id, string Username, string Password) : RecordReference(Id);
    public record RecordReference(Guid Id);

    public record UserCreated(UserReference user) : ICreatedEvent
    {
        public Guid StreamId => user.Id;
    }

    public record UserLoggedIn(UserReference user) : IDatabaseEvent
    {
        public Guid StreamId => user.Id;
    }

    public interface ICreatedEvent : IDatabaseEvent
    {

    }

    public interface IDatabaseEvent
    {
        Guid StreamId { get; }
    }

    public class EventSummary
    {
        public required Guid Id { get; set; }
        public required Guid EventTypeId { get; set; }
        public required DateTimeOffset TimestampUtc { get; set; }
        public required IEnumerable<RecordReference> Records { get; set; }
    }

    public class EventSummaryProjection : SingleStreamProjection<EventSummary, Guid>
    {
        public static EventSummary Create(IEvent<IDatabaseEvent> @event)
        {
            return new EventSummary()
            {
                Id = Guid.CreateVersion7(),
                EventTypeId = @event.Data.GetType().GUID,
                TimestampUtc = @event.Timestamp,
                Records = []
                //Records = @event.Data.EventDescription.Description.Where(x => x.IsRecordReference).Select(x => x.AsRecordReference()),
            };
        }
    }
}
