using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace MarkNic
{
  public class PatientRecOut
  {
    public string xActionId { get; set; }
    public string eventDttm { get; set; }
    public string kafkaTime { get; set; }
    public string processDuration { get; set; }
    public string eventId { get; set; }
    public string changeSequence { get; set; }
    public string eventType { get; set; }
    public string patientId { get; set; }
    public string storeNbr { get; set; }
    public string rxNbr { get; set; }
    public string prevStoreNbr { get; set; }
    public bool prevRxNbr { get; set; }
    public string rxStatusCd { get; set; }
    public string drugId { get; set; }
    public object drugNonSystemCd { get; set; }
    public string drugName { get; set; }
    public string ndc { get; set; }
    public string gpi { get; set; }
    public int rxOriginalQty { get; set; }
    public int rxOriginalQtyDisp { get; set; }
    public int rxOriginalDaysSupply { get; set; }
    public int totalQtyPrescribed { get; set; }
    public int totalQtyDispensed { get; set; }
    public string refillsByDttm { get; set; }
    public object fillAutoInd { get; set; }
    public string fillUnlimitedInd { get; set; }
    public int originCd { get; set; }
    public object cobPlanId { get; set; }
    public string rx90DayPrefInd { get; set; }
    public string rx90DayPrefDttm { get; set; }
    public object rx90DayPrefStatCd { get; set; }
    public object rx90DayPrefStatDttm { get; set; }
  }



  public static class EventHubTrigger1
  {
    [FunctionName("EventHubTrigger1")]
    public static async Task Run(
      [EventHubTrigger("patient-record-processing", Connection = "ADXEGmarknicadx_EVENTHUB", ConsumerGroup = "to-data-processing")] string[] events,
      ILogger log)
    {
      Random random = new Random();
      Stopwatch stopWatch = new Stopwatch();
      stopWatch.Start();

      var exceptions = new List<Exception>();

      foreach (var eventString in events)
      {
        try
        {
          var patientRec = JsonConvert.DeserializeObject<PatientRecOut>(eventString);

          var mseconds = random.Next(50, 1500);
          Thread.Sleep(mseconds);

          // Replace these two lines with your processing logic.
          log.LogInformation($"C# Event Hub trigger function processed a message: {patientRec.xActionId}");

          await Task.Yield();
          stopWatch.Stop();
          // Get the elapsed time as a TimeSpan value.
          TimeSpan ts = stopWatch.Elapsed;

          log.LogInformation($"Message: {patientRec.xActionId} took {ts.Milliseconds} to execute.");
        }
        catch (Exception e)
        {
          // We need to keep processing the rest of the batch - capture this exception and continue.
          // Also, consider capturing details of the message that failed processing so it can be processed again later.
          exceptions.Add(e);
        }
      }

      // Once processing of the batch is complete, if any messages in the batch failed processing throw an exception so that there is a record of the failure.

      if (exceptions.Count > 1)
        throw new AggregateException(exceptions);

      if (exceptions.Count == 1)
        throw exceptions.Single();
    }
  }
}
