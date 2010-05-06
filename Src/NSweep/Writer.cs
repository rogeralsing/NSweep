using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace NSweep.Framework
{
    class Writer
    {
        private readonly int idSize;
        private readonly int dataSize;
        private readonly Stream output;

        public Writer(int idSize, int dataSize, Stream output)
        {
            this.idSize = idSize;
            this.dataSize = dataSize;
            this.output = output;
        }

        public void Write(byte[] id, byte[] data)
        {
            output.Write(id, 0, id.Length);
            output.Write(data, 0, data.Length);
        }
    }
}
