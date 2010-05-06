using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using NSweep.Framework.Messages;

namespace NSweep.Framework
{
    enum EntryAction
    {
        Added,
        Updated,
        Deleted,
    }

    public class DiffSweeper<TI,TD>
    {
        private readonly int idSize;
        private readonly int dataSize;
        private readonly IEnumerable<TD> dataSource;
        private readonly Stream input;
        private readonly Stream output;
        private readonly Func<TI, byte[]> fromIdToBytesConverter;
        private readonly Func<TD, byte[]> fromSourceToBytesConverter;
        private readonly Func<TD, TI> fromSourceToIdConverter;
        private readonly Func<byte[], TI> fromBytesToIdConverter;
        private readonly IMessageSink messageSink;

        private readonly Reader reader;
        private readonly Writer writer;

        public DiffSweeper(int idSize, int dataSize, IEnumerable<TD> dataSource, Stream input, Stream output, Func<TI, byte[]> fromIdToBytesConverter, Func<TD, byte[]> fromSourceToBytesConverter, Func<TD, TI> fromSourceToIdConverter,Func<byte[],TI> fromBytesToIdConverter,IMessageSink messageSink)
        {
            this.idSize = idSize; //raw size of ID/KEY values
            this.dataSize = dataSize; //raw size of Data/Value values
            this.dataSource = dataSource; //data provider
            this.input = input; //cache input stream
            this.output = output; //cache output stream
            this.fromIdToBytesConverter = fromIdToBytesConverter; //converts ID's on <TD> objects into byte[]
            this.fromSourceToBytesConverter = fromSourceToBytesConverter; //converts data or hash from <TD> objects into byte[]
            this.fromSourceToIdConverter = fromSourceToIdConverter; //converts source item to id 
            this.fromBytesToIdConverter = fromBytesToIdConverter; //converts bytes to id
            this.messageSink = messageSink;
            this.reader = new Reader(idSize, dataSize, input);
            this.writer = new Writer(idSize, dataSize, output);
        }

        //equality check for byte[] identities
        private EntryAction CompareIdentities(byte[] sourceId, byte[] cacheId)
        {
            for (int i = 0; i < idSize; i++)
            {
                if (sourceId[i] < cacheId[i])
                    return EntryAction.Added;
                if (sourceId[i] > cacheId[i])
                    return EntryAction.Deleted;
            }
            return EntryAction.Updated;
        }

        public void Run()
        {
            var itemInCache = reader.Read();
            foreach (var itemInSource in dataSource)
            {
                var typedSourceId = fromSourceToIdConverter(itemInSource);
                var sourceId = fromIdToBytesConverter(typedSourceId);
                var sourceData = fromSourceToBytesConverter(itemInSource);
                
                if (itemInCache == null)
                {
                    //input eof
                }
                else
                {
                    var action = CompareIdentities(sourceId, itemInCache.Id);

                    if (action == EntryAction.Deleted)
                    {
                        //handle deletion and step forward to next add or update
                        EntriesDeleted(ref itemInCache, sourceId, ref action);
                    }

                    if (action == EntryAction.Added)
                    {
                        EntryAdded(itemInSource, typedSourceId);
                    }
                    else if (action == EntryAction.Updated)
                    {
                        EntryUpdated(itemInCache, itemInSource, typedSourceId, sourceData);

                        itemInCache = reader.Read();
                    }
                }
                writer.Write(sourceId, sourceData);
            }
        }

        private void EntryUpdated(Entry itemInCache, TD itemInSource, TI typedSourceId, byte[] sourceData)
        {
            bool updated = CompareData(itemInCache.Data, sourceData);

            //only yield if data changed
            if (updated)
            {
                var updatedMessage = new EntryUpdated<TI, TD>()
                {
                    Identity = typedSourceId,
                    Data = itemInSource,
                };
                messageSink.Send(updatedMessage);
            }
        }

        private void EntryAdded(TD itemInSource, TI typedSourceId)
        {
            var addedMessage = new EntryAdded<TI, TD>()
            {
                Identity = typedSourceId,
                Data = itemInSource,
            };
            messageSink.Send(addedMessage);
        }


        private EntryDeleted<TI> CreateDeleteMessage(Entry deletedEntry)
        {
            var deletedId = fromBytesToIdConverter(deletedEntry.Id);
            return new EntryDeleted<TI>()
            {
                Identity = deletedId,
            };
        }

        private void EntriesDeleted(ref Entry itemInCache, byte[] sourceId, ref EntryAction action)
        {
            //current item has been deleted
            messageSink.Send(CreateDeleteMessage(itemInCache));

            //loop to see if more items are deleted
            while (true)
            {
                itemInCache = reader.Read();

                if (itemInCache == null)
                    break;

                action = CompareIdentities(sourceId, itemInCache.Id);
                if (action != EntryAction.Deleted)
                    break;

                messageSink.Send(CreateDeleteMessage(itemInCache));
            }
        }

        private bool CompareData(byte[] cacheData, byte[] sourceData)
        {
            bool updated = false;
            for (int i = 0; i < dataSize; i++)
            {
                if (cacheData[i] != sourceData[i])
                {
                    updated = true;
                    break;
                }
            }
            return updated;
        }
    }
}
