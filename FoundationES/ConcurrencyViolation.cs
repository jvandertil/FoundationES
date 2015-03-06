using System;
using System.Runtime.Serialization;

namespace FoundationES
{
    public class ConcurrencyViolationException : Exception
    {
        public ConcurrencyViolationException()
        {
        }

        public ConcurrencyViolationException(string aggregId, long expectedVersion, long version)
            : base(string.Format("Expected '{0}' to be ver. '{1}' but got '{2}'.", aggregId, expectedVersion, version))
        {
        }

        protected ConcurrencyViolationException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}