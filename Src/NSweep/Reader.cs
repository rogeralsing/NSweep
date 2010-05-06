using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace NSweep.Framework
{
    class Reader
    {
        private readonly int idSize;
        private readonly int dataSize;
        private readonly Stream input;
        
        public Reader(int idSize, int dataSize,Stream input)
        {
            this.idSize = idSize;
            this.dataSize = dataSize;
            this.input = input;
        }

        public Entry Read()
        {
            byte[] currentId = new byte[idSize];
            byte[] currentData = new byte[dataSize];

            var res = input.Read(currentId, 0, idSize);

            if (res == 0)
                return null; //eof

            input.Read(currentData, 0, dataSize);

            return new Entry
            {
                Id = currentId,
                Data = currentData,
            };
        }
    }
}
