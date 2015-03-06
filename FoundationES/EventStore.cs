using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using FoundationDB.Client;

namespace FoundationES
{
    public class EventStore : IEventStore
    {
        private readonly IFdbDatabase _db;
        private readonly FdbSubspace _eventStoreSpace;
        private bool _disposed = false;

        private const int AggregatePrefix = 0x00;
        //private const int GlobalPrefix = 0x01;

        public EventStore(IFdbDatabase db, FdbSubspace eventStoreSpace)
        {
            _eventStoreSpace = eventStoreSpace;
            _db = db;
        }

        public async Task ClearAsync(CancellationToken token)
        {
            await _db.ClearRangeAsync(_eventStoreSpace, token);
        }

        public async Task AppendToAggregateAsync(string aggregId, long expectedVersion, Envelope[] events, CancellationToken token)
        {
            var aggregateSpace = _eventStoreSpace.Partition(AggregatePrefix, aggregId);

            await _db.ReadWriteAsync(async trans =>
            {
                var nextKeyIndex = await GetNextKeyIndexAsync(trans, aggregateSpace);

                switch (expectedVersion)
                {
                    case Constants.ExpectedVersionAny:
                        break;
                    case Constants.ExpectedVersionNone:
                        if (nextKeyIndex != 0)
                        {
                            throw new ConcurrencyViolationException(aggregId, expectedVersion, nextKeyIndex - 1);
                        }
                        break;
                    default:
                        if ((nextKeyIndex - 1) != expectedVersion)
                        {
                            throw new ConcurrencyViolationException(aggregId, expectedVersion, nextKeyIndex - 1);
                        }
                        break;
                }

                for (int i = 0; i < events.Length; ++i)
                {
                    Envelope item = events[i];

                    long aggregateIndex = nextKeyIndex + i;

                    var aggregKey = aggregateSpace.Partition(aggregateIndex, item.ContractName);

                    trans.Set(aggregKey, Slice.Create(item.Data));
                }
            }, token);
        }

        private async Task<long> GetNextKeyIndexAsync(IFdbTransaction trans, FdbSubspace aggregSpace)
        {
            var keyRange = FdbKeyRange.StartsWith(aggregSpace);
            var keySelector = FdbKeySelector.LastLessThan(keyRange.End);

            var lastKey = await trans.GetKeyAsync(keySelector);

            if (lastKey.CompareTo(keyRange.Begin) > 0)
            {
                return aggregSpace.UnpackFirst<long>(lastKey) + 1;
            }

            if (lastKey == Slice.Nil || lastKey == Slice.Empty)
            {
                return 0;
            }

            if (!aggregSpace.Contains(lastKey))
            {
                // TODO: Add metrics here. Event store subspace is completely empty?
                return 0;
            }

            // TODO: Add metrics here.
            return 0;
        }

        public async Task<IEnumerable<AggregateEvent>> ReadAllFromAggregateAsync(string aggregId, CancellationToken token)
        {
            return await _db.QueryAsync(trans =>
            {
                var aggregateSpace = _eventStoreSpace.Partition(AggregatePrefix, aggregId);
                var keyRange = FdbKeyRange.PrefixedBy(aggregateSpace);
                var options = new FdbRangeOptions() { Mode = FdbStreamingMode.WantAll };

                var rangeFuture = trans.Snapshot.GetRange(keyRange, options);

                return rangeFuture.Select(kv =>
                {
                    var key = aggregateSpace.Unpack(kv.Key);
                    var index = key.Get<long>(0);
                    var contract = key.Get<string>(1);

                    return new AggregateEvent(contract, index, kv.Value.GetBytes());
                });
            }, token);
        }
    }
}