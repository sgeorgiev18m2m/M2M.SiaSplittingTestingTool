using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static M2M.SiaSplittingTestingTool.Worker;

namespace M2M.SiaSplittingTestingTool.Contracts
{
    public class SiaEvent
    {
        public SiaEvent()
        {
            
        }
        public SiaEvent(string siaEvent)
        {
            this.Event = siaEvent;
        }
        public SiaEvent(Int64 id, string siaEvent)
        {
            this.Id = id;
            this.Event = siaEvent;
        }
        public Int64? Id { get; set; }
        public string Event { get; set; }
        public  List<string> SplitEvents { get; set; }
        public SiaExitSection ExitSection { get; set; }
    }
}
