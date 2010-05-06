using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace NSweep.Framework
{
    public interface IMessageSink
    {
        void Send<T>(T message);
    }
}
