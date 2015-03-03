using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using FoundationDB.Client;

namespace FoundationES
{
    public class EventStore : IEventStore, IDisposable
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
            if (!token.IsCancellationRequested)
            {
                await _db.ClearRangeAsync(_eventStoreSpace, token);
            }
        }

        public async Task AppendToAggregateAsync(string aggregId, long expectedVersion, IEnumerable<Envelope> events, CancellationToken token)
        {
            await _db.ReadWriteAsync(async trans =>
            {
                var aggregateSpace = _eventStoreSpace.Partition(AggregatePrefix, aggregId);
                var nextKeyIndex = await GetNextKeyIndex(trans, aggregateSpace);

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

                long index = 0;
                foreach (var item in events)
                {
                    long aggregateIndex = nextKeyIndex + index;

                    var aggregKey = aggregateSpace.Partition(aggregateIndex, item.ContractName);

                    trans.Set(aggregKey, Slice.Create(item.Data));

                    ++index;
                }

            }, token);
        }

        private async Task<long> GetNextKeyIndex(IFdbTransaction trans, FdbSubspace aggregSpace)
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
                return 0;
            }

            throw new Exception();
        }

        public async Task<IEnumerable<AggregateEvent>> ReadAllFromAggregateAsync(string aggregId, CancellationToken token)
        {
            return await _db.QueryAsync(trans =>
            {
                var aggregateSpace = _eventStoreSpace.Partition(AggregatePrefix, aggregId);
                var keyRange = FdbKeyRange.PrefixedBy(aggregateSpace);
                var options = new FdbRangeOptions(null, false, null, FdbStreamingMode.WantAll);

                var rangeFuture = trans.GetRange(keyRange, options);

                return rangeFuture.Select(kv =>
                {
                    var key = aggregateSpace.Unpack(kv.Key);
                    var index = key.Get<long>(0);
                    var contract = key.Get<string>(1);

                    return new AggregateEvent(contract, index, kv.Value.GetBytes());
                });
            }, token);
        }

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            _db.Dispose();

            _disposed = true;
        }
    }
}