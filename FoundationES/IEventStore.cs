using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace FoundationES
{
    public interface IEventStore
    {
        Task ClearAsync(CancellationToken token);

        Task AppendToAggregateAsync(string aggregId, long expectedVersion, IEnumerable<Envelope> events, CancellationToken token);

        Task<IEnumerable<AggregateEvent>> ReadAllFromAggregateAsync(string aggregId, CancellationToken token);
    }
}