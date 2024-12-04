using ClosedXML.Excel;
using M2M.SiaSplittingTestingTool.Contracts;
using Microsoft.Extensions.Logging;
using System.Text.RegularExpressions;
using System.Text;
using DocumentFormat.OpenXml.Spreadsheet;

namespace M2M.SiaSplittingTestingTool
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly Configuration? _config;

        public Worker(ILogger<Worker> logger, IConfiguration config)
        {
            _logger = logger;
            _config = config.GetSection("Configuration").Get<Configuration>();
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            bool useFile = !string.IsNullOrEmpty(_config?.LoadEventsPath);

            List<SiaEvent> sia = useFile ? DatabaseManager.GetEventsFromFile(_config!.LoadEventsPath) : DatabaseManager.GetTwoHundredThousandEvents();

            List<SiaEvent> siaSingleEvent = new List<SiaEvent>();
            List<SiaEvent> siaMultipleEvents = new List<SiaEvent>();
            List<SiaEvent> siaNoSplitEvents = new List<SiaEvent>();

            do
            {
                foreach (SiaEvent siaEvent in sia)
                {
                    SplitSIAEvents(siaEvent, out List<string> splitEvents, out SiaExitSection exitSection);
                    siaEvent.SplitEvents = splitEvents;
                    siaEvent.ExitSection = exitSection;

                    if (exitSection == SiaExitSection.OneEventCapture)
                    {
                        siaSingleEvent.Add(siaEvent);
                    }
                    else if (exitSection == SiaExitSection.NoSplit)
                    {
                        siaNoSplitEvents.Add(siaEvent);
                    }
                    else
                    {
                        siaMultipleEvents.Add(siaEvent);
                    }
                }

                if (useFile)
                {
                    CreateXlxsFile(SiaExitSection.OneEventCapture, siaSingleEvent);
                    siaSingleEvent.Clear();

                    CreateXlxsFile(SiaExitSection.MoreEventsMorePartitions, siaMultipleEvents);
                    siaMultipleEvents.Clear();

                    CreateXlxsFile(SiaExitSection.NoSplit, siaNoSplitEvents);
                    siaNoSplitEvents.Clear();

                    break;
                }
                else
                {
                    if (siaSingleEvent.Count > 300000)
                    {
                        CreateXlxsFile(SiaExitSection.OneEventCapture, siaSingleEvent);
                        siaSingleEvent.Clear();
                    }

                    if (siaMultipleEvents.Count > 30000)
                    {
                        CreateXlxsFile(SiaExitSection.MoreEventsMorePartitions, siaMultipleEvents);
                        siaMultipleEvents.Clear();
                    }

                    if (siaNoSplitEvents.Count > 10000)
                    {
                        CreateXlxsFile(SiaExitSection.NoSplit, siaNoSplitEvents);
                        siaNoSplitEvents.Clear();
                    }

                    sia = DatabaseManager.GetTwoHundredThousandEvents();
                }
            }
            while (sia.Count > 0);
        }

        static int WorkbookIndex = 1;
        void CreateXlxsFile(SiaExitSection eventType, List<SiaEvent> siaEvents)
        {
            string fileNameBeginning = "Multiples";

            if (eventType == SiaExitSection.OneEventCapture)
            {
                fileNameBeginning = "Singles";
            }

            if (eventType == SiaExitSection.NoSplit)
            {
                fileNameBeginning = "NoSplit";
            }

            using (var workbook = new XLWorkbook())
            {
                // Add a worksheet
                var worksheet = workbook.Worksheets.Add("Sia");

                // Add headers
                worksheet.Cell(1, 1).Value = "Id";
                worksheet.Cell(1, 2).Value = "Event";
                worksheet.Cell(1, 3).Value = "Output";
                worksheet.Cell(1, 4).Value = "Split Count";
                worksheet.Cell(1, 5).Value = "Split Result";

                int row = 2;

                foreach (SiaEvent siaEvent in siaEvents)
                {
                    // Add data to the columns
                    worksheet.Cell(row, 1).Value = $"{siaEvent.Id}";
                    worksheet.Cell(row, 2).Value = $"{siaEvent.Event}";
                    worksheet.Cell(row, 3).Value = $"{siaEvent.ExitSection}";
                    worksheet.Cell(row, 4).Value = $"{siaEvent.SplitEvents.Count}";
                    worksheet.Cell(row, 5).Value = $"{String.Join(", ", siaEvent.SplitEvents)}";

                    row++;
                }

                // Auto-fit columns for better visibility
                worksheet.Columns().AdjustToContents();

                // Save the Excel file
                var filePath = $"Logs\\{fileNameBeginning}_{DateTime.UtcNow.Month}_{DateTime.UtcNow.Day}_{WorkbookIndex}.xlsx";
                workbook.SaveAs(filePath);
            }
            WorkbookIndex++;
        }
        public enum SiaExitSection
        {
            OneEventCapture = 0,
            MoreEventsOnePartition= 1,
            MoreEventsMorePartitions = 2,
            MoreEventsSecondTryOnePartition = 3,
            MoreEventsSecondTryMorePartitions = 4,
            MoreEventsThirdTry = 5,
            InvalidSia = 6,
            NoSplit = 7
        }
        void SplitSIAEvents(SiaEvent siaEvent, out List<string> splitEvents, out SiaExitSection exitSection)
        {
            string message = siaEvent.Event;

            message = message.Trim();

            splitEvents = new List<string>();

            // Regex logic taken from the code that splits the SIA events in the Dashboard of the RControl Admin Portal
            // code to split a long SIA message into separate atomic SIA events

            if (!message.StartsWith("#"))
            {
                // this is not a SIA message, don't modify it
                splitEvents.Add(message);
                exitSection = SiaExitSection.InvalidSia;
            }
            else
            {
                //#0003|Nri1/OP40/YK00/Ori1/RX00/MA00/MH00/CL40
                message = message.Replace("/Ori", "|Nri");
                //#4875|NRP0|OFA4|OFT3
                message = message.Replace("|O", "|N");

                // #123456|Nri01/TA008*'Zone 8'NM/TA007*'Zone 7'NM/TA006*'Zone 6'NM|Nri01/TA005*'Zone 5'NM|Nri01/TA004*'Zone 4'NM
                //string regexTmplExtended = "^#(?<accountno>[A-Z,0-9]{4,6})([|]N(?<partitions>(?<time>/?ti\\d{1,2}:\\d{1,2})?(?<group>/?ri\\w{1,2})?(?<user>/?id\\w{1,4})?(?<module>/?pi\\w{1,3})?(?<events>/?[A-Z]{2}(?:\\w{1,4})?)+(?<additional>([*]'[^|]*)*))+)+$";
                //string regexTmplExtended = "^#(?<accountno>[A-Z,0-9]{4,6})([|]N(?<partitions>(?<time>/?ti\\d{1,2}:\\d{1,2})?(?<group>/?ri\\w{1,2})?(?<user>/?id\\w{1,4})?(?<module>/?pi\\w{1,3})?((?<events>/?[A-Z]{2}(?:\\w{1,4})?)(?<additional>([*]'[^/|]+)))+)+)+$";
                string regexTmplExtended = "^#(?<accountno>[A-Z,0-9]{4,8})([|]N(?<partitions>(?<aisection>/?ai\\w{1,4})?(?<time>/?ti\\d{1,2}:\\d{1,2}(:\\d{1,2})?)?(?<group>/?ri\\w{1,4})?(?<user>/?id\\w{1,4})?(?<module>/?pi\\w{1,3})?((?<events>/?[A-Z]{2}(?:\\w{1,10})?)(?<additional>([*]'?[^/|]+)))+)+)+$";


                Regex regexExtended = new Regex(regexTmplExtended);
                Match matchExtended = regexExtended.Match(message);

                if (matchExtended != null && matchExtended.Success && matchExtended.Groups["partitions"].Success &&
                    matchExtended.Groups["accountno"].Success && matchExtended.Groups["events"].Success && matchExtended.Groups["events"].Captures != null && matchExtended.Groups["events"].Captures.Count > 1)
                {

                    // This is 95% of all events
                    if (matchExtended.Groups["partitions"].Captures.Count == 1)
                    {
                        exitSection = SiaExitSection.MoreEventsOnePartition;

                        StringBuilder sbLog = new StringBuilder();

                        sbLog.Append("#");
                        sbLog.Append(matchExtended.Groups["accountno"].Value);
                        sbLog.Append("|N");

                        if (matchExtended.Groups["time"].Success)
                        {
                            sbLog.Append(matchExtended.Groups["time"].Value);
                        }

                        if (matchExtended.Groups["group"].Success)
                        {
                            sbLog.Append(matchExtended.Groups["group"].Value);
                        }

                        if (matchExtended.Groups["user"].Success)
                        {
                            sbLog.Append(matchExtended.Groups["user"].Value);
                        }

                        if (matchExtended.Groups["module"].Success)
                        {
                            sbLog.Append(matchExtended.Groups["module"].Value);
                        }

                        for (int i = 0; i < matchExtended.Groups["events"].Captures.Count; i++)
                        {
                            StringBuilder sbLogEvent = new StringBuilder();

                            sbLogEvent.Append(sbLog);

                            sbLogEvent.Append(matchExtended.Groups["events"].Captures[i].Value);

                            if (matchExtended.Groups["additional"].Success)
                            {
                                string additional = matchExtended.Groups["additional"].Captures[i].Value;
                                if (additional.Contains("|A"))
                                {
                                    additional = additional.Replace("|A", "");
                                    sbLogEvent.Append("|A");
                                }
                                sbLogEvent.Append(additional.TrimEnd());
                            }

                            splitEvents.Add(sbLogEvent.ToString());
                        }
                    }
                    else
                    {
                        exitSection = SiaExitSection.MoreEventsMorePartitions;

                        for (int p = 0; p < matchExtended.Groups["partitions"].Captures.Count; p++)
                        {
                            //var regexTmpl1 = "^(?<time>/?ti\\d{1,2}:\\d{1,2})?(?<group>/?ri\\w{1,2})?(?<user>/?id\\w{1,3})?(?<module>/?pi\\w{1,3})?((?<events>/[A-Z]{2}(?:\\w{1,4})?)(?<additional>([*]'[^/]+)))+$";
                            var regexTmpl1 = "^(?<time>/?ti\\d{1,2}:\\d{1,2})?(?<group>/?ri\\w{1,2})?(?<user>/?id\\w{1,4})?(?<module>/?pi\\w{1,3})?((?<events>/[A-Z]{2}(?:\\w{1,10})?)(?<additional>([*]'[^/]+)))+$";

                            Regex regex1 = new Regex(regexTmpl1);
                            Match match1 = regex1.Match(matchExtended.Groups["partitions"].Captures[p].Value);

                            if (match1 != null && match1.Success && match1.Groups["events"].Success && match1.Groups["events"].Captures != null && match1.Groups["events"].Captures.Count > 1)
                            {
                                StringBuilder sbLog = new StringBuilder();

                                sbLog.Append("#");
                                sbLog.Append(matchExtended.Groups["accountno"].Value);
                                sbLog.Append("|N");

                                bool firstPartitionElementAppended = false;

                                if (match1.Groups["time"].Success)
                                {
                                    string time = firstPartitionElementAppended ? match1.Groups["time"].Value : match1.Groups["time"].Value.Replace("/", "");

                                    sbLog.Append(time);
                                    firstPartitionElementAppended = true;
                                }

                                if (match1.Groups["group"].Success)
                                {
                                    string group = firstPartitionElementAppended ? match1.Groups["group"].Value : match1.Groups["group"].Value.Replace("/", "");
                                    sbLog.Append(group);
                                    firstPartitionElementAppended = true;
                                }

                                if (match1.Groups["user"].Success)
                                {
                                    string user = firstPartitionElementAppended ? match1.Groups["user"].Value : match1.Groups["user"].Value.Replace("/", "");

                                    sbLog.Append(user);

                                    firstPartitionElementAppended = true;
                                }

                                if (match1.Groups["module"].Success)
                                {
                                    string module = firstPartitionElementAppended ? match1.Groups["module"].Value : match1.Groups["module"].Value.Replace("/", "");
                                    sbLog.Append(module);

                                    firstPartitionElementAppended = true;
                                }

                                for (int i = 0; i < match1.Groups["events"].Captures.Count; i++)
                                {
                                    StringBuilder sbLogEvent = new StringBuilder();

                                    sbLogEvent.Append(sbLog);

                                    sbLogEvent.Append(match1.Groups["events"].Captures[i].Value);

                                    if (match1.Groups["additional"].Success)
                                    {
                                        string additional = match1.Groups["additional"].Captures[i].Value;
                                        if (additional.Contains("|A"))
                                        {
                                            additional = additional.Replace("|A", "");
                                            sbLogEvent.Append("|A");
                                        }
                                        sbLogEvent.Append(additional.TrimEnd());
                                    }

                                    splitEvents.Add(sbLogEvent.ToString());
                                }
                            }
                            else
                            {
                                StringBuilder sbLog = new StringBuilder();

                                sbLog.Append("#");
                                sbLog.Append(matchExtended.Groups["accountno"].Value);
                                sbLog.Append("|N");

                                sbLog.Append(matchExtended.Groups["partitions"].Captures[p].Value.Replace("/", ""));

                                splitEvents.Add(sbLog.ToString());
                            }
                        }
                    }
                }
                else
                {
                    if (message.Contains("Xmit"))
                    {
                        string patternXmit = @".{2}Xmit";

                        message = Regex.Replace(message, patternXmit, matchReg =>
                        {
                            // |AXmit
                            if (matchReg.Value[0] == '|' && matchReg.Value[1] == 'A')
                            {
                                return matchReg.Value;
                            }
                            // X AXmit
                            else if (matchReg.Value[0] != '|' && matchReg.Value[1] == 'A')
                            {
                                return matchReg.Value[0] + "|AXmit"; // Add "|" before "AXmit"
                            }
                            // XX Xmit
                            else if (matchReg.Value[0] != '|' && matchReg.Value[1] != 'A') 
                            {
                                return matchReg.Value[0] + matchReg.Value[1] + "|AXmit"; // Add "|A" before "Xmit"
                            }
                            else
                            {
                                return matchReg.Value;
                            }
                        });
                    }

                    if (message.Contains("A="))
                    {
                        string patternAEquals = @".{1}?A=";

                        message = Regex.Replace(message, patternAEquals, matchReg =>
                        {
                            if (matchReg.Value[0] != 'A')
                            {
                                return matchReg.Value[0] + "|AA="; //  Add "|A" before "A="
                            }
                            else
                            {
                                return "AA="; // If "|A" already exists, don't change it
                            }
                        });
                    }

                    if (message.Contains("BI="))
                    {
                        string patternBEquals = @".{1}?BI=";

                        message = Regex.Replace(message, patternBEquals, matchReg =>
                        {
                            if (matchReg.Value[0] != 'A')
                            {
                                return matchReg.Value[0] + "|ABI="; //  Add "|A" before "BI="
                            }
                            else
                            {
                                return "ABI="; // If "|A" already exists, don't change it
                            }
                        });
                    }

                    // #0000|NTT10IA|A B0 Z10 Zone 10|NIA|A B0
                    // #0000|NTR10|A B0 Z10 Zone 10|NTR600|A B0 Panel Lid
                    // #1234|Nri10/YT100|ABattery Fault ;|A    |Nri10/LX000|AEngineer Exit ;Area A |Nri10/OR254|AAlarm Silenced ;Input Switched Area A |Nri10/CF254|AForced Set ;Input Switched Area A 
                    //string regexTmpl = "^#(?<accountno>[A-Z,0-9]{4,6})([|]N(?<partitions>(?<time>/?ti\\d{1,2}:\\d{1,2}(:\\d{1,2})?)?(?<group>/?ri\\w{1,4})?(?<user>/?id\\w{1,4})?(?<module>/?pi\\w{1,3})?(?<events>/?[A-Z]{2}(?:\\w{1,10})?)+(?<additional>([|]A[^|]*)*))+)+$";

                    string regexTmpl = "^#(?<accountno>[A-Z,0-9]{4,8})([|]N(?<partitions>(?<aisection>/?ai\\w{1,4})?(?<time>/?ti\\d{1,2}:\\d{1,2}(:\\d{1,2})?)?(?<group>/?ri\\w{1,4})?(?<user>/?id\\w{1,4})?(?<module>/?pi\\w{1,3})?(?<events>/?[A-Z]{2}(?:\\w{1,10})?)+(?<additional>(?:\\^[^\\^]+\\^|[|/]A[^|]*)*))+)+$";

                    Regex regex = new Regex(regexTmpl);
                    Match match = regex.Match(message);

                    if (match != null && match.Success && match.Groups["partitions"].Success &&
                    match.Groups["accountno"].Success && match.Groups["events"].Success && match.Groups["events"].Captures != null && match.Groups["events"].Captures.Count > 1)
                    {
                        // This is 95% of all events
                        if (match.Groups["partitions"].Captures.Count == 1)
                        {
                            exitSection = SiaExitSection.MoreEventsSecondTryOnePartition;
                            StringBuilder sbLog = new StringBuilder();

                            sbLog.Append("#");
                            sbLog.Append(match.Groups["accountno"].Value);
                            sbLog.Append("|N");

                            if (match.Groups["time"].Success)
                            {
                                sbLog.Append(match.Groups["time"].Value);
                            }

                            if (match.Groups["group"].Success)
                            {
                                sbLog.Append(match.Groups["group"].Value);
                            }

                            if (match.Groups["user"].Success)
                            {
                                sbLog.Append(match.Groups["user"].Value);
                            }

                            if (match.Groups["module"].Success)
                            {
                                sbLog.Append(match.Groups["module"].Value);
                            }

                            for (int i = 0; i < match.Groups["events"].Captures.Count; i++)
                            {
                                StringBuilder sbLogEvent = new StringBuilder();

                                sbLogEvent.Append(sbLog);

                                string eventToAppend = match.Groups["events"].Captures[i].Value;

                                if (eventToAppend[0] == '/' && sbLog.ToString().EndsWith("|N"))
                                    eventToAppend = eventToAppend.Replace("/", "");

                                sbLogEvent.Append(eventToAppend);

                                if (match.Groups["additional"].Success)
                                {
                                    string additional = match.Groups["additional"].Value;
                                    if (additional.Contains("|A"))
                                    {
                                        additional = additional.Replace("|A", "");
                                        sbLogEvent.Append("|A");
                                    }
                                    sbLogEvent.Append(additional.TrimEnd());
                                }

                                splitEvents.Add(sbLogEvent.ToString());
                            }
                        }
                        else
                        {
                            exitSection = SiaExitSection.MoreEventsSecondTryMorePartitions;
                            for (int p = 0; p < match.Groups["partitions"].Captures.Count; p++)
                            {
                                //regexTmpl = "^(?<time>/?ti\\d{1,2}:\\d{1,2})?(?<group>/?ri\\w{1,2})?(?<user>/?id\\w{1,4})?(?<module>/?pi\\w{1,3})?(?<events>/?[A-Z]{2}(?:\\w{1,4})?)+(?<additional>([|]A[^|]*)*)$";

                                regexTmpl = "^(?<time>/?ti\\d{1,2}:\\d{1,2})?(?<group>/?ri\\w{1,2})?(?<user>/?id\\w{1,4})?(?<module>/?pi\\w{1,3})?(?<events>/?[A-Z]{2}(?:\\w{1,10})?)+(?<additional>([|/]A[^|]*)*)$";

                                Regex regex1 = new Regex(regexTmpl);
                                Match match1 = regex1.Match(match.Groups["partitions"].Captures[p].Value);

                                if (match1 != null && match1.Success && match1.Groups["events"].Success && match1.Groups["events"].Captures != null && match1.Groups["events"].Captures.Count > 1)
                                {
                                    StringBuilder sbLog = new StringBuilder();

                                    sbLog.Append("#");
                                    sbLog.Append(match.Groups["accountno"].Value);
                                    sbLog.Append("|N");

                                    bool firstPartitionElementAppended = false;

                                    if (match1.Groups["time"].Success)
                                    {
                                        string time = firstPartitionElementAppended ? match1.Groups["time"].Value : match1.Groups["time"].Value.Replace("/", "");

                                        sbLog.Append(time);
                                        firstPartitionElementAppended = true;
                                    }

                                    if (match1.Groups["group"].Success)
                                    {
                                        string group = firstPartitionElementAppended ? match1.Groups["group"].Value : match1.Groups["group"].Value.Replace("/", "");
                                        sbLog.Append(group);
                                        firstPartitionElementAppended = true;
                                    }

                                    if (match1.Groups["user"].Success)
                                    {
                                        string user = firstPartitionElementAppended ? match1.Groups["user"].Value : match1.Groups["user"].Value.Replace("/", "");

                                        sbLog.Append(user);

                                        firstPartitionElementAppended = true;
                                    }

                                    if (match1.Groups["module"].Success)
                                    {
                                        string module = firstPartitionElementAppended ? match1.Groups["module"].Value : match1.Groups["module"].Value.Replace("/", "");
                                        sbLog.Append(module);
                                        firstPartitionElementAppended = true;
                                    }

                                    for (int i = 0; i < match1.Groups["events"].Captures.Count; i++)
                                    {
                                        StringBuilder sbLogEvent = new StringBuilder();

                                        sbLogEvent.Append(sbLog);

                                        string eventToAppend = match.Groups["events"].Captures[i].Value;

                                        if (eventToAppend[0] == '/' && sbLog.ToString().EndsWith("|N"))
                                            eventToAppend = eventToAppend.Replace("/", "");

                                        sbLogEvent.Append(eventToAppend);

                                        if (match1.Groups["additional"].Success)
                                        {
                                            string additional = match1.Groups["additional"].Value;
                                            if (additional.Contains("|A"))
                                            {
                                                additional = additional.Replace("|A", "");
                                                sbLogEvent.Append("|A");
                                            }
                                            sbLogEvent.Append(additional.TrimEnd());
                                        }

                                        splitEvents.Add(sbLogEvent.ToString());
                                    }
                                }
                                else
                                {
                                    StringBuilder sbLog = new StringBuilder();

                                    sbLog.Append("#");
                                    sbLog.Append(match.Groups["accountno"].Value);
                                    sbLog.Append("|N");

                                    //#1234|Nri10/YT100|ABattery Fault ;|ABatt
                                    string partition = match.Groups["partitions"].Captures[p].Value;
                                    int indexIdx = partition.IndexOf("|A");
                                    if (indexIdx != -1)
                                    {
                                        string[] partitionParams = partition.Split(new string[] { "|A" }, StringSplitOptions.RemoveEmptyEntries);
                                        if (partitionParams.Length > 2) // #1234|Nri10/YT100|ABattery Fault ;|ABatt
                                        {
                                            for (int l = 0; l < partitionParams.Length; l++)
                                            {
                                                if (l == 1)
                                                {
                                                    sbLog.Append("|A");
                                                }

                                                sbLog.Append(partitionParams[l]);
                                            }
                                        }
                                        else // #1234|Nri10/YT100|ABattery Fault 
                                        {
                                            sbLog.Append(partition);
                                        }
                                    }
                                    else // #1234|Nri10/YT100
                                    {
                                        sbLog.Append(partition);
                                    }

                                    splitEvents.Add(sbLog.ToString());
                                }
                            }
                        }
                    }
                    // #7541|Nri01/OP002*'Supervisor'NM
                    // #1234|NNT|NYT|NCG1*'B1 Block 1'NM|NCL1*'B0 U01 gosho'NM
                    // We ADD: #1234|NCG1*'B1 Block 1'NM  AND  #1234|NCL1*'B0 U01 gosho'NM
                    else
                    {
                        regexTmpl = "^(?<accountno>#[A-Z,0-9]{4,8})(?<events>[|]N(/?ti\\d{1,2}:\\d{1,2})?(/?ri\\w{1,2})?(/?id\\w{1,4})?(/?pi\\w{1,3})?/?[A-Z]{2}(?:\\w{0,4})?(?:[*]'[^']+'NM)?)+$";

                        Regex regex1 = new Regex(regexTmpl);
                        Match match1 = regex1.Match(message);

                        if (match1 != null && match1.Success && match1.Groups["accountno"].Success &&
                            match1.Groups["events"].Success && match1.Groups["events"].Captures != null && match1.Groups["events"].Captures.Count > 1)
                        {
                            exitSection = SiaExitSection.MoreEventsThirdTry;
                            for (int e = 0; e < match1.Groups["events"].Captures.Count; e++)
                            {
                                StringBuilder sbLogEvent = new StringBuilder();

                                sbLogEvent.Append(match1.Groups["accountno"].Value);
                                sbLogEvent.Append(match1.Groups["events"].Captures[e].Value);

                                splitEvents.Add(sbLogEvent.ToString());
                            }
                        }
                        else
                        {
                            if (matchExtended?.Success is true || match?.Success is true|| match1?.Success is true)
                            {
                                exitSection = SiaExitSection.OneEventCapture;
                            }
                            else
                            {
                                exitSection = SiaExitSection.NoSplit;
                            }
                            //#1234|Nri10/YT100|ABattery Fault ;|ABatt
                            int indexIdx = message.IndexOf("|A");
                            if (indexIdx != -1)
                            {
                                string[] eventParams = message.Split(new string[] { "|A" }, StringSplitOptions.RemoveEmptyEntries);

                                if (eventParams.Length > 2) // #1234|Nri10/YT100|ABattery Fault ;|ABatt
                                {
                                    StringBuilder sbLog = new StringBuilder();
                                    for (int l = 0; l < eventParams.Length; l++)
                                    {
                                        if (l == 1)
                                        {
                                            sbLog.Append("|A");
                                        }

                                        sbLog.Append(eventParams[l]);
                                    }

                                    splitEvents.Add(sbLog.ToString());
                                }
                                else
                                {
                                    splitEvents.Add(message);
                                }
                            }
                            else
                            {
                                splitEvents.Add(message);
                            }
                        }
                    }
                }

                if (splitEvents.Count == 0)
                {
                    // For some reason we could not split the SIA message.
                    // It is possible that the Regex is missing some of the formats, so we will just keep the event unmodified
                    splitEvents.Add(message);

                    exitSection = SiaExitSection.NoSplit;
                }
            }
        }
    }
}