using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;

namespace Monitor
{
    class MainClass
    {
        private static readonly long s_unixTicks = (new DateTime(1970, 1, 1)).Ticks;

        private struct LogData
        {
            public readonly uint Timestamp;
            public readonly string IP;
            public readonly byte Cpu;
            public readonly byte Load;

            public LogData(string line)
            {
                try
                {
                    string[] data = line.Split(new char[] { ' ' }, 4);
                    Timestamp = uint.Parse(data[0]);
                    IP = data[1];
                    Cpu = byte.Parse(data[2]);
                    Load = byte.Parse(data[3]);
                }
                catch (Exception ex)
                {
                    throw new ApplicationException("Something went terribly wrong with last line algo", ex);
                }
            }
        }

        private static IDictionary<string, IList<LogData>> s_allData = new Dictionary<string, IList<LogData>>();

        private static void LoadFiles(int from, int to, string[] files)
        {
            for (int i = from; i < to; i++)
            {
                string[] lines = File.ReadAllLines(files[i]);

                if (lines.Length == 0)
                    return;

#if !PARANOIC
                foreach (string line in lines)
                {
                    LogData data = new LogData(line);

                    IList<LogData> collection;
                    lock(s_allData)
                        if (!s_allData.TryGetValue(data.IP, out collection))
                            s_allData[data.IP] = collection = new List<LogData>();

                    lock(collection)
                        collection.Add(data);
                }
#else
                LogData firstLine = new LogData(lines[0]);

                IList<LogData> collection;

                lock(s_allData)
                    if (!s_allData.TryGetValue(firstLine.IP, out collection))
                        s_allData[firstLine.IP] = collection = new List<LogData>();

                collection.Add(firstLine);

                for (int j = 1; j < lines.Length; j++)
                {
                    LogData data = new LogData(lines[j]);

                    collection.Add(data);
                }
#endif
                Console.WriteLine("Loaded {0}/{1}", i, files.Length);
            }
        }

        private static void LoadData(string folder)
        {
            string[] files = Directory.GetFiles(folder);

            int count = Environment.ProcessorCount;

            int splice = files.Length / count;

            Thread[] threads = new Thread[count - 1];
            for (int i = 0; i <count - 1; i++)
            {
                int from = i * splice;
                int to = (i + 1) * splice;

                threads[i] = new Thread(()=>LoadFiles(from, to, files));
                threads[i].Start();
            }

            LoadFiles((count - 1) * splice, files.Length, files);

            foreach (Thread thread in threads)
                thread.Join();

            Console.WriteLine("Loaded {0} logs", s_allData.Count);

            foreach (IList<LogData> list in s_allData.Values)
                ((List<LogData>)list).Sort((a, b) => a.Timestamp.CompareTo(b.Timestamp));
        }

        private static ICollection<LogData> SelectData(string folder, string ip, DateTime from, DateTime to)
        {
            IList<LogData> collection;

            if (!s_allData.TryGetValue(ip, out collection))
                return new LogData[0];

            uint fromTs = (uint)((from.Ticks - s_unixTicks) / TimeSpan.TicksPerSecond);
            uint toTs = (uint)((to.Ticks - s_unixTicks) / TimeSpan.TicksPerSecond);

            if (collection[0].Timestamp > toTs || collection[collection.Count - 1].Timestamp < fromTs)
                return new LogData[0];

            if (collection[0].Timestamp > fromTs && collection[collection.Count - 1].Timestamp < toTs)
                return collection;

            List<LogData> result = new List<LogData>(collection.Count);

            foreach (LogData data in collection)
            {
                if (data.Timestamp >= fromTs && data.Timestamp <= toTs)
                    result.Add(data);

                if (data.Timestamp > toTs)
                    break;
            }

            return result;
        }

        private static int ProcessLoad(string folder, string ip, int cpu, DateTime from, DateTime to)
        {
            ICollection<LogData> allData = SelectData(folder, ip, from, to);

            int count = 0;
            int result = 0;
            foreach (LogData data in allData)
                if (data.Cpu == cpu)
                {
                    result += data.Load;
                    count++;
                }

            return count == 0 ? 0 : (int)(Math.Round(result / (float)count));
        }

        private static ICollection<Tuple<uint, int>> ProcessQuery(string folder, string ip, int cpu, DateTime from, DateTime to)
        {
            ICollection<LogData> allData = SelectData(folder, ip, from, to);

            List<Tuple<uint, int>> result = new List<Tuple<uint, int>>();

            foreach (LogData data in allData)
                if (data.Cpu == cpu)
                    result.Add(new Tuple<uint, int>(data.Timestamp, data.Load));

            return result;
        }

        private static IDictionary<int, int> ProcessStat(string folder, string ip, DateTime from, DateTime to)
        {
            ICollection<LogData> allData = SelectData(folder, ip, from, to);

            Dictionary<byte, int[]> result = new Dictionary<byte, int[]>();

            foreach (LogData data in allData)
            {
                int[] pair;

                if (!result.TryGetValue(data.Cpu, out pair))
                    result[data.Cpu] = new int[] { data.Load, 1 };
                else
                {
                    pair[0] += data.Load;
                    pair[1]++;
                }
            }

            Dictionary<int, int> aresult = new Dictionary<int, int>();

            foreach (KeyValuePair<byte, int[]> pair in result)
                aresult[pair.Key] = (int)(Math.Round(pair.Value[0] / (float)pair.Value[1]));

            return aresult;
        }

        public static void Main(string[] args)
        {
            if (args.Length < 1)
            {
                Console.Error.WriteLine("Invalid arguments.\nSyntax: Monitor.exe folder\n");
                return;
            }

            string folder = args[0];
            if (!Directory.Exists(folder))
            {
                Console.Error.WriteLine("Folder {0} not found!\nSyntax: Monitor.exe folder\n", folder);
                return;
            }

            LoadData(folder);
            Console.WriteLine("Data loaded");

            bool finished = false;

            while (!finished)
            {
                string[] line = Console.ReadLine().Split(new char[] { ' ' }, 2);

                switch (line[0].ToLower())
                {
                    case null:
                    case "quit":
                    case "exit":
                        finished = true;
                        break;
                    case "load":
                        {
                            string ip;
                            int cpu;
                            DateTime from;
                            DateTime to;

                            try
                            {
                                string[] nargs = line[1].Split(new char[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
                                if (nargs.Length < 6)
                                    throw new ArgumentException();

                                ip = nargs[0];
                                cpu = int.Parse(nargs[1]);
                                from = DateTime.ParseExact(nargs[2] + " " + nargs[3], "yyyy-MM-dd HH:mm", null);
                                to = DateTime.ParseExact(nargs[4] + " " + nargs[5], "yyyy-MM-dd HH:mm", null);
                            }
                            catch // NumberFormatException, IndexOutOfRangeException, etc.
                            {
                                Console.Error.WriteLine("Invalid LOAD command arguments\nSyntax: LOAD IP cpu_id time_start time_end\n");
                                break;
                            }


                            Stopwatch sw = new Stopwatch();
                            sw.Start();

                            int load = ProcessLoad(folder, ip, cpu, from, to);

                            Console.WriteLine("{0}%", load);

                            sw.Stop();

                            Console.WriteLine("\nTime: {0}", sw.Elapsed);
                        }
                        break;
                    case "query":
                        {
                            string ip;
                            int cpu;
                            DateTime from;
                            DateTime to;

                            try
                            {
                                string[] nargs = line[1].Split(new char[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
                                if (nargs.Length < 6)
                                    throw new ArgumentException();

                                ip = nargs[0];
                                cpu = int.Parse(nargs[1]);
                                from = DateTime.ParseExact(nargs[2] + " " + nargs[3], "yyyy-MM-dd HH:mm", null);
                                to = DateTime.ParseExact(nargs[4] + " " + nargs[5], "yyyy-MM-dd HH:mm", null);
                            }
                            catch // NumberFormatException, IndexOutOfRangeException, etc.
                            {
                                Console.Error.WriteLine("Invalid QUERY command arguments\nSyntax: QUERY IP cpu_id time_start time_end\n");
                                break;
                            }

                            Stopwatch sw = new Stopwatch();
                            sw.Start();

                            ICollection<Tuple<uint, int>> query = ProcessQuery(folder, ip, cpu, from, to);

                            foreach (Tuple<uint, int> tuple in query)
                                Console.Write("({0}, {1}%), ", new DateTime(1970, 1, 1).AddSeconds(tuple.Item1).ToString("yyyy-MM-dd HH:mm"), tuple.Item2);

                            sw.Stop();

                            Console.WriteLine("\nTime: {0}", sw.Elapsed);
                        }
                        break;
                    case "stat":
                        {
                            string ip;
                            DateTime from;
                            DateTime to;

                            try
                            {
                                string[] nargs = line[1].Split(new char[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
                                if (nargs.Length < 5)
                                    throw new ArgumentException();

                                ip = nargs[0];
                                from = DateTime.ParseExact(nargs[1] + " " + nargs[2], "yyyy-MM-dd HH:mm", null);
                                to = DateTime.ParseExact(nargs[3] + " " + nargs[4], "yyyy-MM-dd HH:mm", null);
                            }
                            catch // NumberFormatException, IndexOutOfRangeException, etc.
                            {
                                Console.Error.WriteLine("Invalid STAT command arguments\nSyntax: STAT time_start time_end\n");
                                break;
                            }

                            Stopwatch sw = new Stopwatch();
                            sw.Start();

                            IDictionary<int, int> stat = ProcessStat(folder, ip, from, to);

                            foreach (KeyValuePair<int, int> pair in stat)
                                Console.WriteLine("{0}: {1}%", pair.Key, pair.Value);

                            sw.Stop();

                            Console.WriteLine("\nTime: {0}", sw.Elapsed);
                        }
                        break;
                    default:
                        Console.Error.WriteLine("Invalid command.\nSyntax: command <params>\n");
                        break;
                }
            }
        }
    }
}
