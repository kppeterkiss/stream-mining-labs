package bloomfilter;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utilities.BitMap;
import utilities.SetOfHashes;
import utilities.SimpleHashFunction;

import java.util.ArrayList;
import java.util.List;

public class BloomFilterKeyed extends KeyedProcessFunction<Long, String, String> {
    static final Logger log = LoggerFactory.getLogger(BloomFilterKeyed.class);


    transient ValueState<BitMap> bm;
    transient ValueState<SetOfHashes> hashFunctions;
    //BloomFilter
    //BitMap bm;
    int k = 3;
    int n = 10;

    @Override
    public void open(Configuration config){
        ValueStateDescriptor<BitMap> bmd= new ValueStateDescriptor<BitMap>("bm", TypeInformation.of(new TypeHint<BitMap>(){}),new BitMap(n));
        ValueStateDescriptor<SetOfHashes> shfd = new ValueStateDescriptor<SetOfHashes>("sh",TypeInformation.of(new TypeHint<SetOfHashes>() {
        }),new SetOfHashes(k,n));
        bm = getRuntimeContext().getState(bmd);
        hashFunctions = getRuntimeContext().getState(shfd);
    }
    /*
    @Override
    public void flatMap(String value, Collector<String> out) throws Exception {
        boolean free = false;
        List<Integer> values = new ArrayList<>();
        for(SimpleHashFunction f : this.hashFunctions)
            values.add(f.getHash(value));
        for (int v : values) {
            System.out.println(this.bm.toString());
            log.error(String.valueOf(this.bm.get(v)));
            free |= !this.bm.get(v);
            bm.set(v);
        }
        if(free)
            out.collect(value);
    }*/

        @Override
        public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
            SetOfHashes hashes = this.hashFunctions.value();
            BitMap bf = this.bm.value();
            boolean free = false;
            List<Integer> values = new ArrayList<>();
            for(SimpleHashFunction f : hashes.hashes)
                values.add(f.getHash(value));
            for (int v : values) {
                free |= !bf.get(v);
                bf.set(v);
            }
            if(free)
                out.collect(value);
            this.bm.update(bf);

        }
}
