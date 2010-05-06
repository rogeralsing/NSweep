using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace NSweep.Framework
{
    public class FakeMessageSink : IMessageSink
    {
        public void Send<T>(T message)
        {
            Console.WriteLine(message);
        }
    }
}
