using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace NSweep.Framework.Messages
{
    public class EntryDeleted<TI>
    {
        public TI Identity { get; set; }
    }
}
