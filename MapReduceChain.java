import org.apache.hadoop.util.Tool;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.streaming.StreamJob;


public class MapReduceChain extends Configured implements Tool
{
    public int run( String[] args) throws Exception
    {
        JobControl jobControl = new JobControl( "kMeansQuery");

        String[] job1Args = new String[]
        {
            "-mapper"   , "kmapper.py",
            "-reducer"  , "kreducer.py",
            "-input"    , "/data/kmeans_data.csv",
            "-output"   , "/outputk1/",
            "-file"		, "kmapper.py",
            "-file"		, "kreducer.py"
        };
        JobConf job1Conf = StreamJob.createJob(job1Args);
        Job job1 = new Job(job1Conf);
        jobControl.addJob(job1);

        String[] job2Args = new String[]
        {
            "-mapper"   , "kmapper.py",
            "-reducer"  , "kreducer.py",
            "-input"    , "/data/kmeans_data.csv",
            "-output"   , "/outputk2/",
            "-file"		, "kmapper.py",
            "-file"		, "kreducer.py",
            "-file"		, "centers.txt"
        };
        JobConf job2Conf = StreamJob.createJob(job2Args);
        Job job2 = new Job(job2Conf);
        job2.addDependingJob(job1);
        jobControl.addJob(job2);

        String[] job3Args = new String[]
        {
            "-mapper"   , "kmapper.py",
            "-reducer"  , "kreducer.py",
            "-input"    , "/data/kmeans_data.csv",
            "-output"   , "/outputk3/",
            "-file"		, "kmapper.py",
            "-file"		, "kreducer.py",            
            "-file"		, "centers.txt"
        };
        JobConf job3Conf = StreamJob.createJob(job3Args);
        Job job3 = new Job(job3Conf);
        job3.addDependingJob(job2);
        jobControl.addJob(job3);

        String[] job4Args = new String[]
        {
            "-mapper"   , "kmapper.py",
            "-reducer"  , "kreducer.py",
            "-input"    , "/data/kmeans_data.csv",
            "-output"   , "/outputk4/",
            "-file"		, "kmapper.py",
            "-file"		, "kreducer.py",            
            "-file"		, "centers.txt"
        };
        JobConf job4Conf = StreamJob.createJob(job4Args);
        Job job4 = new Job(job4Conf);
        job4.addDependingJob(job3);
        jobControl.addJob(job4);

        Thread runJobControl = new Thread(jobControl);
        runJobControl.start();
        while(!jobControl.allFinished())
        {
            // wait here
        }

        return 0;
    }

    public static void main( String[] args) throws Exception
    {
        int result = ToolRunner.run(new Configuration(), new MapReduceChain(), args);
        System.exit(result);
    }
}
