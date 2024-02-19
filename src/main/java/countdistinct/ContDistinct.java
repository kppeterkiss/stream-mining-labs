package countdistinct;

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
import utilities.SimpleHashFunction;


public class ContDistinct extends KeyedProcessFunction<Long,String,Integer> {


    transient ValueState<BitMap> bm;
    transient ValueState<SimpleHashFunction> hashFunction;

    private static final Logger Log = LoggerFactory.getLogger(ContDistinct.class);

    int bitmap_lenght= 12;

    @Override
    public void open(Configuration config){
        ValueStateDescriptor<BitMap> bmd= new ValueStateDescriptor<BitMap>("bm", TypeInformation.of(new TypeHint<BitMap>(){}),new BitMap(bitmap_lenght));
        ValueStateDescriptor<SimpleHashFunction> shfd = new ValueStateDescriptor<SimpleHashFunction>("sh",TypeInformation.of(new TypeHint<SimpleHashFunction>() {
        }),new SimpleHashFunction(bitmap_lenght));
        bm = getRuntimeContext().getState(bmd);
        hashFunction = getRuntimeContext().getState(shfd);
    }



    public int getTailLength1(String binString){
        char[] ca = binString.toCharArray();
        int i = 0;
        while (ca[ca.length-1-i]!='1')
            i++;
        return i;
    }

    public int getTailLength(String binString){
        char[] ca = binString.toCharArray();
        int tailingZeros = 0;
        for (int i= ca.length-1;i>=0;i--)
            if(ca[i]=='0')
                tailingZeros++;
            else
                return tailingZeros;
        if (tailingZeros==ca.length) return 0;
        return tailingZeros;
    }

    public int getR(BitMap bm){
        int i = 0;
        while (!bm.get(i)) i++;
        return bm.getLength()-i-1;
    }

    @Override
    public void processElement(String value, Context ctx, Collector<Integer> out) throws Exception {
        BitMap bm_ = bm.value();
        //Log.error("String ",value );
        Log.error("After "+value+":" );
        //System.out.println(value);
        SimpleHashFunction hashFunction_= hashFunction.value();
        String binary = Integer.toBinaryString(hashFunction_.getHash(value));
        System.out.println("Input: "+value+"; Hash: "+hashFunction_.getHash(value)+"; Binary: " + binary);

        //System.out.println("Tail length : "+getTailLength1(binary));
        System.out.println("Before:  "+ bm_.toString());
        bm_.set(bitmap_lenght-1-getTailLength(binary));
        System.out.println("After:   "+ bm_.toString());


        int estimate = (int)Math.pow(2,getR(bm_));
        //Log.error("Estimate",estimate );
        //Log.error(Integer.toString(estimate) );
        bm.update(bm_);
        out.collect(Integer.valueOf(estimate));


    }
}
