using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using ProtoBuf;
using System.IO;
using System.Reactive.Linq;
namespace NSweep
{
    public class MessageEventArgs : EventArgs
    {
        public Message Original { get; set; }
        public Message Message { get; set; }
    }

    public class Engine
    {
        private string name;
        private string storagePath;
        private int index = 0;
        private IObservable<Message> sourceMessages;
        private IEnumerator<Message> hashMessages;
        private Message hashMessage;

        public event EventHandler<MessageEventArgs> Inserted;
        public event EventHandler<MessageEventArgs> Updated;
        public event EventHandler<MessageEventArgs> Deleted;

        public Engine(string name, string storagePath, IEnumerable <Message> source) : this(name,storagePath,source.ToObservable())
        {

        }
        public Engine(string name, string storagePath, IObservable<Message> source)
        {
            this.name = name;
            this.storagePath = storagePath;
            this.sourceMessages = source;                      
        }

        public void Subscribe()
        {
            string hashPath = string.Format("{0}\\{1}.dat", this.storagePath, this.name);
            string writePath = string.Format("{0}\\{1}.foo", this.storagePath, this.name);

            var hashStream = new FileStream(hashPath, FileMode.OpenOrCreate);
            var writeStream = new FileStream(writePath, FileMode.Create);

            this.hashMessages = GetHashEnumerator(hashStream).GetEnumerator();

            GetNextHash();

            sourceMessages.Subscribe(sourceMessage =>
                {
                    var keySource = Key.Fixed(sourceMessage.Key, sourceMessage.Key.Length);

                    while (true)
                    {
                        if (hashMessage == null)
                        {
                            //insert
                            OnInserted(sourceMessage);
                            Write(writeStream, sourceMessage);
                            break;
                        }
                        else
                        {
                            var keyHash = Key.Fixed(hashMessage.Key, hashMessage.Key.Length);
                            var keyResult = keySource.CompareTo(keyHash);

                            if (keyResult == 0)
                            {
                                //updated
                                if (!sourceMessage.Body.SequenceEqual(hashMessage.Body))
                                    OnUpdated(sourceMessage,hashMessage);

                                Write(writeStream, sourceMessage);
                                GetNextHash();
                                break;
                            }
                            else if (keyResult < 0)
                            {
                                //insert
                                OnInserted(sourceMessage);
                                Write(writeStream, sourceMessage);
                                break;
                            }
                            else if (keyResult > 0)
                            {
                                //hash was deleted
                                OnDeleted(hashMessage);
                                GetNextHash();
                            }
                        }
                    }
                },

                x =>
                {
                    //TODO: hantera exception
                    writeStream.Dispose();
                    hashStream.Dispose();

                }, () =>
                {
                    //clean up

                    while (hashMessage != null)
                    {
                        OnDeleted(hashMessage);
                        GetNextHash();
                    }

                    writeStream.Dispose();
                    hashStream.Dispose();

                    File.Delete(hashPath);
                    File.Move(writePath, hashPath);

                });
        }

        private void GetNextHash()
        {
            bool hasMore = hashMessages.MoveNext();
            if (hasMore)
                this.hashMessage = hashMessages.Current;
            else
                this.hashMessage = null;
        }

        private void OnUpdated(Message message,Message original)
        {
            if (Updated != null)
                Updated(this, new MessageEventArgs { Message = message , Original = original});
        }

        private void OnInserted(Message message)
        {
            if (Inserted != null)
                Inserted(this, new MessageEventArgs { Message = message });
        }

        private void OnDeleted(Message message)
        {
            if (Deleted != null)
                Deleted(this, new MessageEventArgs { Message = message });
        }

        private void Write(Stream writeStream, Message message)
        {
            index++;

            if (index % 1000 == 0)
                Console.WriteLine(index);

            Serializer.SerializeWithLengthPrefix(writeStream, message, PrefixStyle.Fixed32);
        }

        private IEnumerable<Message> GetHashEnumerator(Stream hashStream)
        {
            while (hashStream.Position < hashStream.Length)
            {
                var message = Serializer.DeserializeWithLengthPrefix<Message>(hashStream, PrefixStyle.Fixed32);
                yield return message;
            }
        }
    }
}
