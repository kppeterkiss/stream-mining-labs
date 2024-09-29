package bloomfilter;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BloomFilterProgram {
    static final Logger log = LoggerFactory.getLogger(BloomFilterProgram.class);

    public static void main(String[] args) throws Exception {

        
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            //DataStream<String> inputstream = env.readTextFile("src/main/resources/inputEmails");
        //ncat -lk 9999
        DataStream<String> inputstream =  env.socketTextStream("localhost", 9999);
        //DataStream<String> filtered = inputstream.flatMap(new BloomFilter2(3,10));


        KeyedStream< String,Long> keyed = inputstream.keyBy(new KeySelector<String, Long>() {
            @Override
            public Long getKey(String value) throws Exception {
                return (long)value.charAt(0);
            }
        });
        DataStream< String> filtered = keyed.process(new BloomFilterKeyed());

        //ncat -lk 5060
        filtered.writeToSocket("localhost", 5060,
                new SerializationSchema<String>() {
                    @Override
                    public byte[] serialize(String element) {
                        return (element). getBytes ();
                    }
                });
        // simplest sink: print
        //filtered.print();
        /*filtered.addSink(new SinkFunction<String>() {
            @Override
            public void invoke(String value) throws Exception {
                log.error(value);  ;
                //System.out.println(value);
            }
        });*/
        env.execute("Bloom filter");


    }
}
