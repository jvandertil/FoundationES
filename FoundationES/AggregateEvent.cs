namespace FoundationES
{
    public class AggregateEvent
    {
        public string ContractName { get; private set; }
        public byte[] Data { get; private set; }
        public long Index { get; private set; }

        public AggregateEvent(string contractName, long index, byte[] data)
        {
            ContractName = contractName;
            Data = data;
            Index = index;
        }
    }
}