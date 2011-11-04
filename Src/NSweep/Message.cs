using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using ProtoBuf;

namespace NSweep
{
    [ProtoContract]
    public class Message
    {
        [ProtoMember(1)]
        public byte[] Key;
        [ProtoMember(2)]
        public byte[] Body;
    }
}
