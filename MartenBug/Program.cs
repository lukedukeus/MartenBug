using JasperFx;
using JasperFx.Events;

using Marten;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

using NetTopologySuite.Utilities;

using Spectre.Console;

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
                });
            })
           .Build();

            IQuerySession querySession = host.Services.GetRequiredService<IQuerySession>();
            IDocumentStore documentStore = host.Services.GetRequiredService<IDocumentStore>();

            int testEvents = 10;
            var userId = Guid.CreateVersion7();

            await using (var session = documentStore.LightweightSession())
            {
                var username = $"User";
                var password = "super secret";

                var userCreatedEvent = new UserCreated(userId, username, password);

                var stream = session.Events.StartStream(userCreatedEvent);

                await session.SaveChangesAsync();

                userId = stream.Id;

                for (int i = 1; i < testEvents; i++)
                {
                    session.Events.Append(userId, new UserUpdated(userId, $"User {i}"));
                    await session.SaveChangesAsync();
                }
            }

            // Works fine
            var userEventsByRequiredProps = await querySession.Events.QueryAllRawEvents()
                .Where(x => x.StreamId == userId)
                .OrderBy(x => x.Version)
                .Select(x => new AuditEventWithRequiredProps()
                {
                    EventId = x.Id,
                    RecordVersion = x.Version,
                    TimestampUtc = x.Timestamp,
                    Name = "TODO",
                    Description = "TODO"
                })
                .ToListAsync();

            // Throws System.InvalidOperationException: 'Sequence contains no elements'
            var userEventsByPrimaryConstructor = await querySession.Events.QueryAllRawEvents()
                .Where(x => x.StreamId == userId)
                .OrderBy(x => x.Version)
                .Select(x => new AuditEventWithPrimaryConstructor(x))
                .ToListAsync();

            Assert.Equals(testEvents, userEventsByPrimaryConstructor.Count);
            Assert.Equals(testEvents, userEventsByRequiredProps.Count);
        }
    }

    public record UserCreated(Guid Id, string Username, string Password);
    public record UserUpdated(Guid Id, string Username);

    public class AuditEventWithPrimaryConstructor(IEvent @event)
    {
        public Guid EventId { get; set; } = @event.Id;
        public long RecordVersion { get; set; } = @event.Version;
        public DateTimeOffset TimestampUtc { get; set; } = @event.Timestamp;
        public string Name { get; set; } = "TODO";
        public string Description { get; set; } = "TODO";
    }

    public class AuditEventWithRequiredProps
    {
        public required Guid EventId { get; set; }
        public required long RecordVersion { get; set; }
        public required DateTimeOffset TimestampUtc { get; set; }
        public required string Name { get; set; } = "TODO";
        public required string Description { get; set; } = "TODO";
    }
}
