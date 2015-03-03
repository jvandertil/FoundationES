namespace FoundationES
{
    public class Envelope
    {
        public string ContractName { get; private set; }

        public byte[] Data { get; private set; }

        public Envelope(string contractName, byte[] data)
        {
            ContractName = contractName;
            Data = data;
        }
    }
}