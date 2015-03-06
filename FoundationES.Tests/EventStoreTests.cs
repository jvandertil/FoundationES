using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FoundationDB.Client;
using FoundationDB.Layers.Tuples;
using NUnit.Framework;

namespace FoundationES.Tests
{
    public class EventStoreTests
    {
        [TestFixture]
        public class ClearAsync
        {
            private IFdbDatabase _db;
            private EventStore _eventStore;
            private FdbSubspace _subspace;
            private const string AggregId = "test/1";
            private const string AggregId2 = "otherAggreg";

            [SetUp]
            public void Setup()
            {
                _db = Fdb.OpenAsync().Result;
                _subspace = FdbSubspace.Create(FdbTuple.Create("FoundationES.ClearAsyncTest"));

                _eventStore = new EventStore(_db, _subspace);
            }

            private async Task AddEventsToSubspace(FdbSubspace space)
            {
                var t1 = _db.WriteAsync(trans => trans.Set(space, FdbTuple.Create(AggregId), Slice.Create(new byte[100])), CancellationToken.None);
                var t2 = _db.WriteAsync(trans => trans.Set(space, FdbTuple.Create(AggregId2), Slice.Create(new byte[100])), CancellationToken.None);

                await Task.WhenAll(t1, t2);
            }

            private async Task AssertSubspaceHasData(FdbSubspace space)
            {
                var beforeErase = await _db.QueryAsync(trans => trans.GetRange(FdbKeyRange.PrefixedBy(space)), CancellationToken.None);
                Assert.That(beforeErase.Any());
            }

            private async Task AssertSubspaceIsEmpty(FdbSubspace space)
            {
                var afterErase = await _db.QueryAsync(trans => trans.GetRange(FdbKeyRange.PrefixedBy(space)), CancellationToken.None);

                Assert.That(!afterErase.Any());
            }

            [Test]
            public async Task Removes_All_Streams()
            {
                await AddEventsToSubspace(_subspace);

                await AssertSubspaceHasData(_subspace);

                await _eventStore.ClearAsync(CancellationToken.None);

                await AssertSubspaceIsEmpty(_subspace);
            }

            [Test]
            public async Task Does_Not_Touch_Other_Subspaces()
            {
                var subspace = FdbSubspace.Create(FdbTuple.Create("FoundationES.ClearAsyncTest2"));

                var es = new EventStore(_db, subspace);

                await Task.WhenAll(
                    AddEventsToSubspace(subspace),
                    AddEventsToSubspace(_subspace));

                await Task.WhenAll(
                    AssertSubspaceHasData(subspace),
                    AssertSubspaceHasData(_subspace));

                await _eventStore.ClearAsync(CancellationToken.None);

                await Task.WhenAll(
                    AssertSubspaceHasData(subspace),
                    AssertSubspaceIsEmpty(_subspace));

                // Cleanup
                await es.ClearAsync(CancellationToken.None);

            }

            [TearDown]
            public void TearDown()
            {
                _db.Dispose();

                _eventStore = null;
                _db = null;
            }
        }
    }
}
