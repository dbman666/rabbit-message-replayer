using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using RabbitMQ.Client;
using System.Diagnostics;


namespace rabbit
{
    class Program
    {
        private static string rabbitHeaderBytes = "rabbit_framing_amqp_0_9_1";
        private static int lenRabbitHeader = rabbitHeaderBytes.Length;
        private static string rabbitHeaderHexChars = BitConverter.ToString(Encoding.ASCII.GetBytes(rabbitHeaderBytes)); // 72-61-62-62-69-74-5F-66-72-61-6D-69-6E-67-5F-61-6D-71-70-5F-30-5F-39-5F-31
        private static byte[] tmpInt32Swap = new byte[4];
        private static byte[] tmpInt64Swap = new byte[8];
        private static string prefixBytes = "3'url";
        private static string prefixHexChars = BitConverter.ToString(Encoding.ASCII.GetBytes(prefixBytes)); // 33-27-75-72-6C
        private static string singleQuoteHex = "27";
        private static int nbCharsInHex = 3;

#if false
        private static bool useCommoneQueue = true;
        private static string commonQueue = "yanick";
        private static string folder = @"C:\data\__Maintenance\replay-rabbit-messages\rabbit";
        private static char toExtension = '-';
        private static bool outputEveryMessage = false;
        private static string rabbitUrl = "amqp://localhost:5672";
        private static string rabbitUser = "guest";
        private static string rabbitPassword = "guest";
#else
        private static bool useCommoneQueue = false;
        private static string commonQueue = "document.processor.pool";
        private static string folder = @".";
        private static char toExtension = '-';
        private static bool outputEveryMessage = false;
        private static string rabbitUrl = Environment.GetEnvironmentVariable("RABBIT_URL");
        private static string rabbitUser = Environment.GetEnvironmentVariable("RABBIT_USER");
        private static string rabbitPassword = Environment.GetEnvironmentVariable("RABBIT_PASSWORD");
#endif

        static int Main(string[] args)
        {
            HandleArgs(args, out var hasApply, out var onlyPush, out var wildcard);

            //Console.WriteLine(wildcard);
            //foreach (var file in System.IO.Directory.EnumerateFiles(folder, wildcard + ".rdq"))
            //    Console.WriteLine(file);

            if (string.IsNullOrWhiteSpace(rabbitUrl)) {
                Console.Error.WriteLine($"Missing RABBIT_URL env var. Ex: 'amqp://queueserver.prod.cloud.coveo.com:5672'");
                return PrintHelp();
            }
            if (string.IsNullOrWhiteSpace(rabbitUser)) {
                Console.Error.WriteLine($"Missing RABBIT_USER env var. Ex: 'svc_rabbit_2'");
                return PrintHelp();
            }
            if (string.IsNullOrWhiteSpace(rabbitPassword)) {
                Console.Error.WriteLine($"Missing RABBIT_PASSWORD env var. Ex: '@donutLover2'");
                return PrintHelp();
            }


            var files = Directory.EnumerateFiles(folder, wildcard).ToList();
            var globalStats = new GlobalStats();
            Console.Error.WriteLine($"{files.Count} files to process.");

            if (hasApply) {
                var iFile = 0;
                var iMsg = 0;
                var connectionFactory = new ConnectionFactory { Uri = new Uri(rabbitUrl), UserName = rabbitUser, Password = rabbitPassword, AutomaticRecoveryEnabled = true, RequestedHeartbeat = 30, SocketWriteTimeout = 300000 };
                using (var connection = connectionFactory.CreateConnection())
                using (var channel = connection.CreateModel()) {
                    var propsPersistent = channel.CreateBasicProperties();
                    propsPersistent.DeliveryMode = 2;

                    foreach (var file in files) {
                        Console.Error.WriteLine($"{iFile}/{files.Count}: {file}");
                        var isRdqFile = file.EndsWith(".rdq");
                        var fileAsBytes = File.ReadAllBytes(file);

                        globalStats.AddFile(file);

                        foreach (var msg in ExtractFromFile(fileAsBytes, isRdqFile)) {
                            globalStats.AddMsg(msg);

                            if (!hasApply)
                                continue;

                            if (onlyPush && !msg.IsPush)
                                continue;

                            Console.WriteLine($"{msg.PosLen}: {msg.MsgLen}: {msg.Queue} push:{msg.IsPush}");

                            if (outputEveryMessage)
                                File.WriteAllBytes($@"c:\temp\for-Yanick-{iMsg}.bin", msg.Bytes);
                            ++iMsg;

                            IBasicProperties props = propsPersistent;
                            if (msg.IsPush) {
                                props = channel.CreateBasicProperties();
                                propsPersistent.DeliveryMode = 2;
                                props.Headers = new Dictionary<string, object>();
                                props.Headers.Add("cmf", $"{{url:{msg.Queue},method:Process,zip:true}}");
                            }
                            channel.BasicPublish("", useCommoneQueue ? commonQueue : msg.Queue, false, props, msg.Bytes);
                        }

                        ++iFile;
                        File.Move(file, file.Substring(0, file.Length - 1) + toExtension);
                    }
                }
            } else {
                var iFile = 0;
                foreach (var file in files) {
                    Console.Error.WriteLine($"{iFile}/{files.Count}: {file}");
                    var isRdqFile = file.EndsWith(".rdq");
                    var fileAsBytes = File.ReadAllBytes(file);

                    globalStats.AddFile(file);

                    foreach (var msg in ExtractFromFile(fileAsBytes, isRdqFile)) {
                        globalStats.AddMsg(msg);
                    }
                    ++iFile;
                }
            }

            OutputJsonStats(globalStats);

            return 0;
        }

        static void HandleArgs(string[] args, out bool p_HasApply, out bool p_OnlyPush, out string p_Wildcard)
        {
            p_HasApply = false;
            p_OnlyPush = false;
            p_Wildcard = "*.rdq";

            for (int i = 0; i < args.Length; ++i) {
                var arg = args[i].ToLower();
                if (arg == "--replay") {
                    p_HasApply = true;
                } else if (arg == "--onlypush") {
                    p_OnlyPush = true;
                } else if (arg == "--help") {
                    PrintHelp();
                } else {
                    p_Wildcard = args[i];
                }
            }
        }

        static int PrintHelp()
        {
            Console.WriteLine(
@"rabbit [--replay] [--onlypush] [<wildcard>]
--replay is the safeguard to NOT replay automatically. It not specified, only stats are output.
<wildcard> must contain the extension. Ex: *.rdq

Expected env vars:
    RABBIT_URL      Ex: amqp://queueserver.prod.cloud.coveo.com:5672
    RABBIT_USER     Ex: svc_rabbit_2
    RABBIT_PASSWORD Ex: @donutLover2
");
            return 1;
        }

        static void OutputJsonStats(GlobalStats p_Stats)
        {
            Console.WriteLine(@"{{""NbFiles"":{0}, ""Queues"":{{", p_Stats.NbFiles);
            int i = 0;
            foreach (var qs in p_Stats.Queues) {
                Console.WriteLine(@"{4}""{0}"":{{ ""NbTotal"":{1}, ""NbPush"":{2}, ""LenTotal"":{3} }}",
                    qs.Key, qs.Value.NbTotal, qs.Value.NbPush, qs.Value.LenTotal, i++ != 0 ? ", " : "");
            }
            Console.WriteLine("}}");
        }

        static IEnumerable<Msg> ExtractFromFile(byte[] fileAsBytes, bool p_UseMsgLen)
        {
            var fileAsHexChars = BitConverter.ToString(fileAsBytes);
            var posLen = 0L;
            var msgLen = 0L;

            while (posLen < fileAsBytes.Length) {
                if (p_UseMsgLen) {
                    msgLen = ExtractInt64(fileAsBytes, (int)posLen);
                    posLen += 8;
                }

                var msgPos = fileAsHexChars.IndexOf(rabbitHeaderHexChars, (int)(posLen * nbCharsInHex));
                if (msgPos == -1)
                    break;
                var posData = msgPos / nbCharsInHex + lenRabbitHeader;
                if (fileAsBytes[posData++] != 'l') throw new Exception("Expected 'l'");
                var nbBlobs = ExtractInt32(fileAsBytes, posData);
                posData += 4;

                var allBlobs = new List<byte>();
                while (nbBlobs-- > 0) {
                    if (fileAsBytes[posData++] != 'm') throw new Exception("Expected 'm'");
                    var blobLen = ExtractInt32(fileAsBytes, posData);
                    posData += 4;
                    var blobBytes = new byte[blobLen];
                    Array.Copy(fileAsBytes, posData, blobBytes, 0, blobLen);
                    allBlobs.InsertRange(0, blobBytes);
                    posData += blobLen;
                    if (!p_UseMsgLen) {
                        msgLen = blobLen;
                        if (nbBlobs != 0) throw new Exception("Expected only one blob when reading from an index file.");
                    }
                }

                var msgBytes = allBlobs.ToArray();
                var msg = new Msg { Bytes = msgBytes, PosLen = posLen, MsgLen = msgLen };

                if (msgBytes[0] == 'i') {
                    msg.Queue = ExtractQueueName(fileAsBytes, fileAsHexChars, msgPos);
                } else {
                    msg.IsPush = true;
                    msg.Queue = ExtractPushQueueName(fileAsBytes, msgPos / nbCharsInHex);
                }
                yield return msg;

                if (p_UseMsgLen && fileAsBytes[posLen] != 0xFF) Console.WriteLine("bad marker");
                posLen = posData + 1;
            }
        }

        static int ExtractInt32(byte[] fileAsBytes, int pos)
        {
            Array.Copy(fileAsBytes, pos, tmpInt32Swap, 0, 4);
            Array.Reverse(tmpInt32Swap);
            return BitConverter.ToInt32(tmpInt32Swap, 0);
        }

        static long ExtractInt64(byte[] fileAsBytes, int pos)
        {
            Array.Copy(fileAsBytes, pos, tmpInt64Swap, 0, 8);
            Array.Reverse(tmpInt64Swap);
            return BitConverter.ToInt64(tmpInt64Swap, 0);
        }

        static string ExtractQueueName(byte[] fileAsBytes, string fileAsHexChars, int startPos)
        {
            // i81i{4'1waybt3'url41'ces.gmproductione2yq29g7-koflgbm4.Dpm.Doc3'veru20171020u3'zipbt}
            var pos = fileAsHexChars.IndexOf(prefixHexChars, startPos);
            var posStart = fileAsHexChars.IndexOf(singleQuoteHex, pos + nbCharsInHex * 5);
            posStart += nbCharsInHex;
            var posEnd = fileAsHexChars.IndexOf(singleQuoteHex, posStart);
            var len = (posEnd - posStart) / nbCharsInHex - 1;
            return Encoding.ASCII.GetString(fileAsBytes, posStart / nbCharsInHex, len);
        }

        static string ExtractPushQueueName(byte[] fileAsBytes, int startPos)
        {
            // {url:ces.gmproductione2yq29g7-koflgbm4.Dpm.Doc,method:Process,zip:true}....rabbit_framing_amqp_0_9_1
            // startPos is pointing on 'rabbit_...'
            var posOpenBrace = startPos;
            while (fileAsBytes[posOpenBrace--] != '{') {
            }
            ++posOpenBrace;
            if (fileAsBytes[++posOpenBrace] != 'u' ||
                fileAsBytes[++posOpenBrace] != 'r' ||
                fileAsBytes[++posOpenBrace] != 'l' ||
                fileAsBytes[++posOpenBrace] != ':') throw new Exception("Expected 'url:'");
            ++posOpenBrace;
            var posCloseBrace = posOpenBrace;
            while (fileAsBytes[++posCloseBrace] != ',') {
            }
            return Encoding.ASCII.GetString(fileAsBytes, posOpenBrace, posCloseBrace - posOpenBrace);
        }
    }

    public class Msg
    {
        public string Queue;
        public byte[] Bytes;
        public bool IsPush;
        public long PosLen;
        public long MsgLen;
    }

    public class GlobalStats
    {
        public int NbFiles;
        public Dictionary<string, QueueStat> Queues = new Dictionary<string, QueueStat>();

        public void AddFile(string p_File)
        {
            ++NbFiles;
        }

        public void AddMsg(Msg p_Msg)
        {
            if (!Queues.TryGetValue(p_Msg.Queue, out var qs)) {
                qs = new QueueStat();
                Queues[p_Msg.Queue] = qs;
            }
            ++qs.NbTotal;
            qs.LenTotal += p_Msg.MsgLen;
            if (p_Msg.IsPush)
                ++qs.NbPush;
        }
    }

    public class QueueStat
    {
        public int NbPush;
        public int NbTotal;
        public long LenTotal;
    }
}
