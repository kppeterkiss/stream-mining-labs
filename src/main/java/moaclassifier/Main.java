package moaclassifier;

import com.yahoo.labs.samoa.instances.Instance;
import moa.classifiers.Classifier;
import moa.classifiers.trees.HoeffdingTree;
import moa.core.Example;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
//import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Flink program for training classifier
 */
public class Main {

    // Flink pipeline
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Example<Instance>> input = env.addSource(new ExampleSource());
        //new wy: SideOutputs
        //https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/side_output/
        final OutputTag< Example<Instance>> trainTag = new OutputTag< Example<Instance>>("train"){};
        final OutputTag< Example<Instance>> testTag = new OutputTag< Example<Instance>>("test"){};

        SingleOutputStreamOperator<Example<Instance>> mainDataStream = input
                .process(new ProcessFunction<Example<Instance>, Example<Instance>>() {

                    @Override
                    public void processElement(
                            Example<Instance> value,
                            Context ctx,
                            Collector< Example<Instance> > out) throws Exception {

                        if (Math.random()<0.95) {
                            ctx.output(trainTag, value);
                        } else {
                            ctx.output(testTag, value);
                        }
                    }});
        DataStream<Example<Instance>> testStream  = mainDataStream.getSideOutput(testTag);
        DataStream<Example<Instance>> trainStream = mainDataStream.getSideOutput(trainTag);

        //https://stackoverflow.com/questions/70101132/apache-flink-spiltstream-vs-side-outputs
        /* Deprecated solution with SplitStreams and dataStream.split(..)
        // tagging the elents using an OutputSelector
        //SplitStream<Example<Instance>> trainAndTest = input.split(new RandomSamplingSelector(0.02))   ;
        // pick data with given tags
        DataStream<Example<Instance>> testStream = trainAndTest.select("test");
        DataStream<Example<Instance>> trainStream = trainAndTest.select("train");
         */
        // call a process function on the training data, that will train our classifier and emit time-by time  a new one
        // that is turning stream of training data into stream of classifiers
        DataStream<Classifier> classifiers = trainStream.process(new LearnProcessFunction(HoeffdingTree.class,1000));
        //make one stream from the testStram and classifier-stream, use the last seen classifier on the test data, then on  a window of 1000 element count how many times we made a good prediction.
        testStream.connect(classifiers).flatMap(new ClassifyAndUpdateFunction()).countWindowAll(1000).aggregate(new ParformanceFunction()).print();
        //execute the pipeline
        env.execute("");

    }

}
