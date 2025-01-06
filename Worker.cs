using ClosedXML.Excel;
using M2M.SiaSplittingTestingTool.Contracts;
using Microsoft.Extensions.Logging;
using System.Text.RegularExpressions;
using System.Text;
using DocumentFormat.OpenXml.Spreadsheet;
using System.Reflection.Metadata;
using DocumentFormat.OpenXml.InkML;

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
                    DateTime dateTimeNow = DateTime.UtcNow; 
                    SplitSIAEvents(siaEvent, out List<string> splitEvents, out SiaExitSection exitSection);

                    TimeSpan duration = DateTime.UtcNow - dateTimeNow;

                    siaEvent.Duration = duration;

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

                    if (duration > TimeSpan.FromMilliseconds(100))
                    {
                        CreateXlxsFile(SiaExitSection.TimeOut, [siaEvent]);
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

                    if (siaNoSplitEvents.Count > 100000)
                    {
                        CreateXlxsFile(SiaExitSection.NoSplit, siaNoSplitEvents);
                        siaNoSplitEvents.Clear();
                    }

                    sia = DatabaseManager.GetTwoHundredThousandEvents(sia[sia.Count - 1].Id);
                }
            }
            while (sia.Count > 0);

            if (siaSingleEvent.Count > 0)
            {
                CreateXlxsFile(SiaExitSection.OneEventCapture, siaSingleEvent);
                siaSingleEvent.Clear();
            }

            if (siaMultipleEvents.Count > 0)
            {
                CreateXlxsFile(SiaExitSection.MoreEventsMorePartitions, siaMultipleEvents);
                siaMultipleEvents.Clear();
            }

            if (siaNoSplitEvents.Count > 0)
            {
                CreateXlxsFile(SiaExitSection.NoSplit, siaNoSplitEvents);
                siaNoSplitEvents.Clear();
            }
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

            if (eventType == SiaExitSection.TimeOut)
            {
                fileNameBeginning = "TimeOut";
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
                worksheet.Cell(1, 6).Value = "Split Time";

                int row = 2;

                foreach (SiaEvent siaEvent in siaEvents)
                {
                    // Add data to the columns
                    worksheet.Cell(row, 1).Value = $"{siaEvent.Id}";
                    worksheet.Cell(row, 2).Value = $"{siaEvent.Event}";
                    worksheet.Cell(row, 3).Value = $"{siaEvent.ExitSection}";
                    worksheet.Cell(row, 4).Value = $"{siaEvent.SplitEvents.Count}";
                    worksheet.Cell(row, 5).Value = $"{String.Join(", ", siaEvent.SplitEvents)}";
                    worksheet.Cell(row, 6).Value = $"{siaEvent.Duration}";

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
            NoSplit = 7,
            TimeOut = 8
        }

        const string accountNoRegex = @"#(?<accountno>[a-zA-Z0-9]{1,8})";
        const string labelRegex = @"(\^[^\\^]*\^)|([*]'?[^/|]+)";
        const string aiRegex = @"(?<aisection>(/?ai\w{1,4})(?<aiLabel>" + labelRegex + @")*)?";
        const string dateTimeRegex = @"(?<date>/?da\d{1,2}-\d{1,2}-\d{1,2})?(?<time>/?ti\d{1,2}:\d{1,2}(:\d{1,2})?)?";
        const string groupRegex = @"(?<group>(/?ri\w{1,4})(?<groupLabel>" + labelRegex + @")*)?";
        const string userRegex = @"(?<user>(/?id\w{1,4})(?<userLabel>" + labelRegex + @")*)?";
        const string moduleRegex = @"(?<module>(/?pi\w{1,4})(?<moduleLabel>" + labelRegex + @")*)?";
        const string eventLabelRegex = @"(?<eventLabel>" + labelRegex + @"|([*]'[^']+'NM?)" + @")*";
        const string eventsRegex = @"(?<events>" + @"(?<eventWithoutLabel>/?[A-Z]{2}(?:\w{1,10})?)" + eventLabelRegex + @")";

        //const string additionalRegex = @"(?<additional>([|/]A[^|]*)*)";
        const string additionalRegex = @"(?<additional>(([|/]A[^|]*))*)";
        //const string additional2Regex = @"(?<additional>[*]'[^']+'NM)?";

        void SplitSIAEvents(SiaEvent siaEvent, out List<string> splitEvents, out SiaExitSection exitSection)
        {
            string message = siaEvent.Event;

            Console.WriteLine("Entering SplitSIAEvents for event " + message);

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

                // #5585|NFA0210/FA0216/|NYR0000/FJ0030
                message = message.Replace("/|N", "|N");

                message = RemoveEndingSlash(message);

                // #123456|Nri01/TA008*'Zone 8'NM/TA007*'Zone 7'NM/TA006*'Zone 6'NM|Nri01/TA005*'Zone 5'NM|Nri01/TA004*'Zone 4'NM
                //string regexTmplExtended = "^#(?<accountno>[A-Z,0-9]{4,6})([|]N(?<partitions>(?<time>/?ti\\d{1,2}:\\d{1,2})?(?<group>/?ri\\w{1,2})?(?<user>/?id\\w{1,4})?(?<module>/?pi\\w{1,3})?(?<events>/?[A-Z]{2}(?:\\w{1,4})?)+(?<additional>([*]'[^|]*)*))+)+$";
                //string regexTmplExtended = "^#(?<accountno>[A-Z,0-9]{4,6})([|]N(?<partitions>(?<time>/?ti\\d{1,2}:\\d{1,2})?(?<group>/?ri\\w{1,2})?(?<user>/?id\\w{1,4})?(?<module>/?pi\\w{1,3})?((?<events>/?[A-Z]{2}(?:\\w{1,4})?)(?<additional>([*]'[^/|]+)))+)+)+$";

                //string regexTmplExtended = "^#(?<accountno>[a-z,A-Z,0-9]{3,8})([|]N(?<partitions>(?<aisection>/?ai\\w{1,4})?(?<time>/?ti\\d{1,2}:\\d{1,2}(:\\d{1,2})?)?(?<group>/?ri\\w{1,4})?(?<user>/?id\\w{1,4})?(?<module>/?pi\\w{1,3})?((?<events>/?[A-Z]{2}(?:\\w{1,10})?)(?<additional>([*]'?[^/|]+)))+)+)+$";

                //string regexTmplExtended = "^#(?<accountno>[a-z,A-Z,0-9]{3,8})([|]N(?<partitions>(?<aisection>/?ai\\w{1,4})?(?<time>/?ti\\d{1,2}:\\d{1,2}(:\\d{1,2})?)?(?<group>/?ri\\w{1,4})?(?<user>/?id\\w{1,4})?(?<module>/?pi\\w{1,3})?((?<events>/?[A-Z]{2}(?:\\w{1,10})?)(?<additional>(\\^[^\\\\^]+\\^)|([*]'?[^/|]+)))+)+)+$";
                string partitionSectionRegex = aiRegex + dateTimeRegex + groupRegex + userRegex + moduleRegex + eventsRegex;

                bool successSplit = SplitSIAEvents(message, partitionSectionRegex, splitEvents, out exitSection);
                if (successSplit)
                {
                    // This is the default SIA protocol sequence /ri/id/pi
                }
                else
                {
                    // Check the rarely found sequence /pi/id/ri
                    partitionSectionRegex = aiRegex + dateTimeRegex + moduleRegex + userRegex + groupRegex + eventsRegex;
                    successSplit = SplitSIAEvents(message, partitionSectionRegex, splitEvents, out exitSection);
                }

                if (!successSplit)
                {
                    splitEvents.Add(message);
                }
            }
        }

        private static bool SplitSIAEvents(string message, string partitionSectionRegex, List<string> splitEvents, out SiaExitSection exitSection)
        {
            bool success = false;
            string regexTmplExtended = @"^" + accountNoRegex + @"([|]N(?<partitions>" + partitionSectionRegex + @")+)+$";

            Regex regexExtended = new Regex(regexTmplExtended);
            Match matchExtended = regexExtended.Match(message);

            if (matchExtended != null && matchExtended.Success && matchExtended.Groups["partitions"].Success &&
                matchExtended.Groups["accountno"].Success && matchExtended.Groups["events"].Success && matchExtended.Groups["events"].Captures != null && matchExtended.Groups["events"].Captures.Count > 1)
            {

                // This is 95% of all events
                if (matchExtended.Groups["partitions"].Captures.Count == 1)
                {
                    exitSection = SiaExitSection.MoreEventsOnePartition;

                    string eventPrefix = GenerateEventPrefix(matchExtended, matchExtended);

                    for (int i = 0; i < matchExtended.Groups["events"].Captures.Count; i++)
                    {
                        string eventCapture = matchExtended.Groups["events"].Captures[i].Value;

                        string? additionalCapture = null;
                        if (matchExtended.Groups["additional"].Success)
                        {
                            additionalCapture = matchExtended.Groups["additional"].Captures[i].Value;
                        }

                        string splitEvent = GenerateEvent(eventPrefix, eventCapture, additionalCapture);
                        splitEvents.Add(splitEvent);
                    }
                }
                else
                {
                    exitSection = SiaExitSection.MoreEventsMorePartitions;

                    for (int p = 0; p < matchExtended.Groups["partitions"].Captures.Count; p++)
                    {
                        //var regexTmpl1 = "^(?<time>/?ti\\d{1,2}:\\d{1,2})?(?<group>/?ri\\w{1,2})?(?<user>/?id\\w{1,4})?(?<module>/?pi\\w{1,3})?((?<events>/[A-Z]{2}(?:\\w{1,10})?)(?<additional>([*]'[^/]+)))+$";
                        //var regexTmpl1 = "^(?<time>/?ti\\d{1,2}:\\d{1,2}(:\\d{1,2})?)?(?<group>/?ri\\w{1,4})?(?<user>/?id\\w{1,4})?(?<module>/?pi\\w{1,3})?((?<events>/?[A-Z]{2}(?:\\w{1,10})?)(?<additional>(\\^[^\\\\^]+\\^)|([*]'?[^/|]+)))+$";                             

                        Regex regex1 = new Regex(partitionSectionRegex);
                        Match match1 = regex1.Match(matchExtended.Groups["partitions"].Captures[p].Value);

                        if (match1 != null && match1.Success && match1.Groups["events"].Success && match1.Groups["events"].Captures != null && match1.Groups["events"].Captures.Count > 1)
                        {
                            string eventPrefix = GenerateEventPrefix(matchExtended, match1);

                            for (int i = 0; i < match1.Groups["events"].Captures.Count; i++)
                            {
                                string eventCapture = match1.Groups["events"].Captures[i].Value;

                                string? additionalCapture = null;
                                if (match1.Groups["additional"].Success)
                                {
                                    additionalCapture = match1.Groups["additional"].Value;
                                }

                                string splitEvent = GenerateEvent(eventPrefix, eventCapture, additionalCapture);
                                splitEvents.Add(splitEvent);
                            }
                        }
                        else
                        {
                            StringBuilder sbLog = new StringBuilder();

                            sbLog.Append("#");
                            sbLog.Append(matchExtended.Groups["accountno"].Value);
                            sbLog.Append("|N");

                            sbLog.Append(RemoveLeadingSlash(matchExtended.Groups["partitions"].Captures[p].Value));

                            splitEvents.Add(sbLog.ToString());
                        }
                    }
                }

                success = true;
            }
            else
            {
                // |Nri006/BR319AI=Hrsk 5.H Inngangur|Nri006/BA319DI=Hrsk 5.H Svaedi 1
                // !!!should be
                // |Nri006/BR319A|AI=Hrsk 5.H Inngangur|Nri006/BA319D|AI=Hrsk 5.H Svaedi 1

                // #9322|Nri020/TA003D|AA=24t kerfissvaedi I=T02 Cabinet

                // |Nai9/CA002A=Heildsala U=.User 65529|Nri001/OP0998A=Lyfjaver U=Eva Mara r~ir|Nri001/BR000BI=Dyrlaesing skrifstofa 1h
                // Should be!!!!
                // |Nai9/CA002|AA=Heildsala U=.User 65529|Nri001/OP0998|AA=Lyfjaver U=Eva Mara r~ir|Nri001/BR000B|AI=Dyrlaesing skrifstofa 1h

                // |Nri003/OP0597U=Kort 10196
                // Should be!!!
                // |Nri003/OP0597|AU=Kort 10196
                message = AppendMissingAdditionalSectionHeader(message, "Xmit");
                message = AppendMissingAdditionalSectionHeader(message, "A=");
                message = AppendMissingAdditionalSectionHeader(message, "U=");
                message = AppendMissingAdditionalSectionHeader(message, "I=");
                //message = AppendMissingAdditionalSectionHeader(message, "BI=");
                //message = AppendMissingAdditionalSectionHeader(message, "AI=");
                //message = AppendMissingAdditionalSectionHeader(message, "DI=");

                // #0000|NTT10IA|A B0 Z10 Zone 10|NIA|A B0
                // #0000|NTR10|A B0 Z10 Zone 10|NTR600|A B0 Panel Lid
                // #1234|Nri10/YT100|ABattery Fault ;|A    |Nri10/LX000|AEngineer Exit ;Area A |Nri10/OR254|AAlarm Silenced ;Input Switched Area A |Nri10/CF254|AForced Set ;Input Switched Area A 
                //string regexTmpl = "^#(?<accountno>[A-Z,0-9]{4,6})([|]N(?<partitions>(?<time>/?ti\\d{1,2}:\\d{1,2}(:\\d{1,2})?)?(?<group>/?ri\\w{1,4})?(?<user>/?id\\w{1,4})?(?<module>/?pi\\w{1,3})?(?<events>/?[A-Z]{2}(?:\\w{1,10})?)+(?<additional>([|]A[^|]*)*))+)+$";

                //string regexTmpl = "^#(?<accountno>[A-Z,0-9]{4,8})([|]N(?<partitions>(?<aisection>/?ai\\w{1,4})?(?<time>/?ti\\d{1,2}:\\d{1,2}(:\\d{1,2})?)?(?<group>/?ri\\w{1,4})?(?<user>/?id\\w{1,4})?(?<module>/?pi\\w{1,3})?(?<events>/?[A-Z]{2}(?:\\w{1,10})?)+(?<additional>(?:\\^[^\\^]+\\^|[|/]A[^|]*)*))+)+$";

                string partitionAndAdditionalSectionRegex = partitionSectionRegex + additionalRegex;
                string regexTmpl = @"^" + accountNoRegex + @"([|]N(?<partitions>" + partitionAndAdditionalSectionRegex + @")+)+$";

                Regex regex = new Regex(regexTmpl);
                Match match = regex.Match(message);

                if (match != null && match.Success && match.Groups["partitions"].Success &&
                match.Groups["accountno"].Success && match.Groups["events"].Success && match.Groups["events"].Captures != null && match.Groups["events"].Captures.Count > 1)
                {
                    // This is 95% of all events
                    if (match.Groups["partitions"].Captures.Count == 1)
                    {
                        exitSection = SiaExitSection.MoreEventsSecondTryOnePartition;

                        string eventPrefix = GenerateEventPrefix(match, match);

                        for (int i = 0; i < match.Groups["events"].Captures.Count; i++)
                        {
                            string eventCapture = match.Groups["events"].Captures[i].Value;

                            string? additionalCapture = null;
                            if (match.Groups["additional"].Success)
                            {
                                additionalCapture = match.Groups["additional"].Value;
                            }

                            string splitEvent = GenerateEvent(eventPrefix, eventCapture, additionalCapture);
                            splitEvents.Add(splitEvent);
                        }
                    }
                    else
                    {
                        exitSection = SiaExitSection.MoreEventsSecondTryMorePartitions;
                        for (int p = 0; p < match.Groups["partitions"].Captures.Count; p++)
                        {
                            //regexTmpl = "^(?<time>/?ti\\d{1,2}:\\d{1,2})?(?<group>/?ri\\w{1,2})?(?<user>/?id\\w{1,4})?(?<module>/?pi\\w{1,3})?(?<events>/?[A-Z]{2}(?:\\w{1,4})?)+(?<additional>([|]A[^|]*)*)$";

                            //regexTmpl = @"^(?<time>/?ti\\d{1,2}:\\d{1,2})?(?<group>/?ri\\w{1,2})?(?<user>/?id\\w{1,4})?(?<module>/?pi\\w{1,3})?(?<events>/?[A-Z]{2}(?:\\w{1,10})?)+(?<additional>([|/]A[^|]*)*)$";

                            Regex regex1 = new Regex(partitionAndAdditionalSectionRegex);
                            Match match1 = regex1.Match(match.Groups["partitions"].Captures[p].Value);

                            if (match1 != null && match1.Success && match1.Groups["events"].Success && match1.Groups["events"].Captures != null && match1.Groups["events"].Captures.Count > 1)
                            {
                                string eventPrefix = GenerateEventPrefix(match, match1);

                                for (int i = 0; i < match1.Groups["events"].Captures.Count; i++)
                                {
                                    string eventCapture = match.Groups["events"].Captures[i].Value;

                                    string? additionalCapture = null;
                                    if (match1.Groups["additional"].Success)
                                    {
                                        additionalCapture = match1.Groups["additional"].Value;
                                    }

                                    string splitEvent = GenerateEvent(eventPrefix, eventCapture, additionalCapture);
                                    splitEvents.Add(splitEvent);
                                }
                            }
                            else
                            {
                                StringBuilder sbLog = new StringBuilder();

                                sbLog.Append("#");
                                sbLog.Append(match.Groups["accountno"].Value);
                                sbLog.Append("|N");

                                //#1234|Nri10/YT100|ABattery Fault ;|ABatt
                                string partition = RemoveLeadingSlash(match.Groups["partitions"].Captures[p].Value);
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

                    success = true;
                }
                // #7541|Nri01/OP002*'Supervisor'NM
                // #1234|NNT|NYT|NCG1*'B1 Block 1'NM|NCL1*'B0 U01 gosho'NM
                // We ADD: #1234|NCG1*'B1 Block 1'NM  AND  #1234|NCL1*'B0 U01 gosho'NM
                else
                {
                    // This is now the same as the previous match
                    //regexTmpl = @"^" + accountNoRegex + @"([|]N(?<partitions>" + aiRegex + dateTimeRegex + groupRegex + userRegex + moduleRegex + eventsRegex + additionalRegex + @")+)+$";

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

                        success = true;
                    }
                    else
                    {
                        if (matchExtended?.Success is true || match?.Success is true || match1?.Success is true)
                        {
                            exitSection = SiaExitSection.OneEventCapture;
                            success = true;

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
                        else
                        {
                            exitSection = SiaExitSection.NoSplit;
                            success = false;
                        }
                    }
                }
            }

            if (splitEvents.Count == 0)
            {
                // For some reason we could not split the SIA message.
                // It is possible that the Regex is missing some of the formats, so we will just keep the event unmodified
                //splitEvents.Add(message);

                exitSection = SiaExitSection.NoSplit;
                success = false;
            }

            return success;
        }
        static string RemoveLeadingSlash(string s)
        {
            if (string.IsNullOrEmpty(s))
            {
                return s;
            }

            if (s.StartsWith("/"))
            {
                return s.Substring(1);
            }
            else
            {
                return s;
            }
        }
        static string RemoveEndingSlash(string s)
        {
            if (string.IsNullOrEmpty(s))
            {
                return s;
            }

            if (s.EndsWith("/") || s.EndsWith("|"))
            {
                return s.Substring(0, s.Length - 1);
            }
            else
            {
                return s;
            }
        }
        static string AppendMissingAdditionalSectionHeader(string message, string pattern)
        {
            // For example, if pattern is "Xmit"
            if (message.Contains(pattern))
            {
                // check the two symbols before the pattern
                string patternRegex = @".{2}" + pattern;

                message = Regex.Replace(message, patternRegex, matchReg =>
                {
                    // We have white space at index 1 fo "U=", so we do nothing
                    // |Nai9/CA002A=Heildsala U=.User 65529

                    // We do not have white space before "A=", so we will try to normalize
                    // |Nri001/OP0998A=Lyfjaver U=Eva Mara r~ir
                    if (matchReg.Value[1] == ' ')
                    {
                        return matchReg.Value;
                    }
                    // |AXmit
                    else if (matchReg.Value[0] == '|' && matchReg.Value[1] == 'A')
                    {
                        return matchReg.Value; // exactly matching the pattern with |A in front of it
                    }
                    // ?AXmit
                    else if (matchReg.Value[0] != '|' && matchReg.Value[1] == 'A')
                    {
                        return matchReg.Value[0].ToString() + "|A" + pattern; // Add "|" before "AXmit"
                    }
                    // #9030|NYC|A=== ? === RS485 - in this message, we should recognize the |A pattern, not the A= pattern
                    else if (matchReg.Value[1] == '|' && matchReg.Value[2] == 'A' && matchReg.Value[3] == '=')
                    {
                        return matchReg.Value;
                    } 
                    // ??Xmit
                    else if (matchReg.Value[0] != '|' && matchReg.Value[1] != 'A')
                    {
                        // .ToString() is mandatory!!! char is converted to int
                        return matchReg.Value[0].ToString() + matchReg.Value[1].ToString() + "|A" + pattern; // Add "|A" before "Xmit"
                    }
                    // |?Xmit
                    else
                    {
                        return matchReg.Value; // Unrecognized, don't modify it
                    }
                });
            }
            return message;
        }
        static string GenerateEventPrefix(Match match, Match match1)
        {
            StringBuilder sbLog = new StringBuilder();

            sbLog.Append("#");
            sbLog.Append(match.Groups["accountno"].Value);
            sbLog.Append("|N");

            bool firstPartitionElementAppended = false;

            if (match1.Groups["time"].Success)
            {
                string time = firstPartitionElementAppended ? match1.Groups["time"].Value : RemoveLeadingSlash(match1.Groups["time"].Value);

                sbLog.Append(time);
                firstPartitionElementAppended = true;
            }

            if (match1.Groups["group"].Success)
            {
                string group = firstPartitionElementAppended ? match1.Groups["group"].Value : RemoveLeadingSlash(match1.Groups["group"].Value);
                sbLog.Append(group);
                firstPartitionElementAppended = true;
            }

            if (match1.Groups["user"].Success)
            {
                string user = firstPartitionElementAppended ? match1.Groups["user"].Value : RemoveLeadingSlash(match1.Groups["user"].Value);

                sbLog.Append(user);

                firstPartitionElementAppended = true;
            }

            if (match1.Groups["module"].Success)
            {
                string module = firstPartitionElementAppended ? match1.Groups["module"].Value : RemoveLeadingSlash(match1.Groups["module"].Value);
                sbLog.Append(module);
                firstPartitionElementAppended = true;
            }

            return sbLog.ToString();
        }
        static string GenerateEvent(string eventPrefix, string eventCapture, string? additionalCapture)
        {
            StringBuilder sbLogEvent = new StringBuilder();

            sbLogEvent.Append(eventPrefix);

            string eventToAppend = eventCapture;

            if (eventPrefix.EndsWith("|N"))
                eventToAppend = RemoveLeadingSlash(eventToAppend);

            sbLogEvent.Append(eventToAppend);

            if (additionalCapture != null)
            {
                string additional = additionalCapture;
                if (additional.Contains("|A"))
                {
                    additional = additional.Replace("|A", "");
                    sbLogEvent.Append("|A");
                }
                sbLogEvent.Append(additional.TrimEnd());
            }

            return sbLogEvent.ToString();
        }
    }
}
