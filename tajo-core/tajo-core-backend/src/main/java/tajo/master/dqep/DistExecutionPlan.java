package tajo.master.dqep;

/**
 * A physical execution plan which contains logical operator, data flow, and data channel.
 */
public class DistExecutionPlan {

  public class Table {
    String rel_id;
  }

  public enum SourceType {
    HDFS,
    LOCAL_HASH,
    LOCAL_RANGE
  };

  public enum DistributionType {
    BROADCAST
  }


  public class OutputChannel {

  }
}
