package bloomfilter;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utilities.BitMap;
import utilities.SimpleHashFunction;

import java.util.ArrayList;
import java.util.List;

public class BloomFilter2 implements FlatMapFunction<String, String> {
    static final Logger log = LoggerFactory.getLogger(BloomFilter2.class);

    BitMap bm;
    int k = 3;
    int n = 10;
    List<SimpleHashFunction> hashFunctions;

    public BloomFilter2(int k, int n){
        this.bm = new BitMap(n);
        this.hashFunctions = new ArrayList<>();
        for(int i = 0;i < k;i++)
            this.hashFunctions.add(new SimpleHashFunction(n));

    }
    @Override
    public void flatMap(String value, Collector<String> out) throws Exception {
        boolean free = false;
        List<Integer> values = new ArrayList<>();
        for(SimpleHashFunction f : this.hashFunctions)
            values.add(f.getHash(value));
        for (int v : values) {
           if (!this.bm.get(v))
               free = true;
            bm.set(v);
        }
        if(free)
            out.collect(value);
    }

}
