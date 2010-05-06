using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace NSweep.Framework.Messages
{
    public class EntryData<TI, TD>
    {
        public TI Identity { get; set; }
        public TD Data { get; set; }
    }
}
