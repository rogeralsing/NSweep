using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using ProtoBuf;
using System.IO;

namespace NSweep
{
    public class MessageEventArgs : EventArgs
    {
        public Message Message { get; set; }
    }

    public class Engine
    {
        private string name;
        private string storagePath;
        private int index = 0;
        private IEnumerable<Message> sourceMessages;
        private IEnumerator<Message> hashMessages;
        private Message hashMessage;

        public event EventHandler<MessageEventArgs> Inserted;
        public event EventHandler<MessageEventArgs> Updated;
        public event EventHandler<MessageEventArgs> Deleted;
        
        public Engine(string name, string storagePath, IEnumerable<Message> source)
        {
            this.name = name;
            this.storagePath = storagePath;
            this.sourceMessages = source;                      
        }

        public void Sweep()
        {
            string hashPath = string.Format("{0}\\{1}.dat", this.storagePath, this.name);
            string writePath = string.Format("{0}\\{1}.foo", this.storagePath, this.name);

            using (var hashStream = new FileStream(hashPath, FileMode.OpenOrCreate))
            using (var writeStream = new FileStream(writePath, FileMode.Create))
            {
                this.hashMessages = GetHashEnumerator(hashStream).GetEnumerator();  

                GetNextHash();

                foreach (var sourceMessage in sourceMessages)
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
                                    OnUpdated(sourceMessage);

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
                }

                while (hashMessage != null)
                {
                    OnDeleted(hashMessage);
                    GetNextHash();
                }
            }
            
            File.Delete(hashPath);
            File.Move(writePath, hashPath);
        }

        private void GetNextHash()
        {
            bool hasMore = hashMessages.MoveNext();
            if (hasMore)
                this.hashMessage = hashMessages.Current;
            else
                this.hashMessage = null;
        }

        private void OnUpdated(Message message)
        {
            if (Updated != null)
                Updated(this, new MessageEventArgs { Message = message });
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
