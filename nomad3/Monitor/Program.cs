using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;

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

        private static ICollection<LogData> SelectData(string folder, string ip, DateTime from, DateTime to)
        {
            string file = Path.Combine(folder, ip + ".log");

            if (!File.Exists(file))
                return new LogData[0];

            uint fromTs = (uint)((from.Ticks - s_unixTicks) / TimeSpan.TicksPerSecond);
            uint toTs = (uint)((to.Ticks - s_unixTicks) / TimeSpan.TicksPerSecond);

            string[] lines = File.ReadAllLines(file);

            List<LogData> result = new List<LogData>(lines.Length);

            foreach (string line in lines)
            {
                LogData data = new LogData(line);
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
            if (args.Length < 2)
            {
                Console.Error.WriteLine("Invalid arguments.\nSyntax: Monitor.exe folder command <params>\n");
                return;
            }

            string folder = args[0];
            if (!Directory.Exists(folder))
            {
                Console.Error.WriteLine("Folder {0} not found!\nSyntax: Monitor.exe folder command <params>\n", folder);
                return;
            }

            string command = args[1];

            switch (command.ToLower())
            {
                case "load":
                    {
                        string ip;
                        int cpu;
                        DateTime from;
                        DateTime to;

                        try
                        {
                            if (args.Length < 6)
                                throw new ArgumentException();
                            
                            ip = args[2];
                            cpu = int.Parse(args[3]);
                            from = DateTime.ParseExact(args[4], "yyyy-MM-dd HH:mm", null);
                            to = DateTime.ParseExact(args[5], "yyyy-MM-dd HH:mm", null);
                        }
                        catch // NumberFormatException, IndexOutOfRangeException, etc.
                        {
                            Console.Error.WriteLine("Invalid LOAD command arguments\nSyntax: Monitor.exe folder LOAD IP cpu_id time_start time_end\n");
                            return;
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
                            if (args.Length < 6)
                                throw new ArgumentException();

                            ip = args[2];
                            cpu = int.Parse(args[3]);
                            from = DateTime.ParseExact(args[4], "yyyy-MM-dd HH:mm", null);
                            to = DateTime.ParseExact(args[5], "yyyy-MM-dd HH:mm", null);
                        }
                        catch // NumberFormatException, IndexOutOfRangeException, etc.
                        {
                            Console.Error.WriteLine("Invalid QUERY command arguments\nSyntax: Monitor.exe folder QUERY IP cpu_id time_start time_end\n");
                            return;
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
                            if (args.Length < 5)
                                throw new ArgumentException();

                            ip = args[2];
                            from = DateTime.ParseExact(args[3], "yyyy-MM-dd HH:mm", null);
                            to = DateTime.ParseExact(args[4], "yyyy-MM-dd HH:mm", null);
                        }
                        catch // NumberFormatException, IndexOutOfRangeException, etc.
                        {
                            Console.Error.WriteLine("Invalid STAT command arguments\nSyntax: Monitor.exe folder STAT time_start time_end\n");
                            return;
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
                    Console.Error.WriteLine("Invalid command.\nSyntax: Monitor.exe folder command <params>\n");
                    return;
            }
        }
    }
}
