package advanced;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class SpeedLimitSource extends RichParallelSourceFunction<Integer> {
    private boolean running  = true;
    private Integer[] speedLimits = new Integer[] {85, 80, 75, 70, 65, 60};
    private static final List<String> carBrandsList = new ArrayList<>();
    static {
        carBrandsList.add("Ford");
        carBrandsList.add("BMW");
        carBrandsList.add("Chevrolet");
        carBrandsList.add("Cadillac");
        carBrandsList.add("Buick");
        carBrandsList.add("Toyota");
        carBrandsList.add("Honda");
        carBrandsList.add("Mazda");
    }


    @Override
    public void run(SourceContext<Tuple2<String, Integer>> sourceContext) throws Exception {
        Random rand = new Random();
        while(running) {
            for (int i = 0; i < speedLimits.length; i++) {
                Thread.sleep(10000);
                for(String carBrand: carBrandsList) {
                    sourceContext.collect(Tuple2.of(carBrand, speedLimits[i] + (int)Math.round(rand.nextGaussian() * 5)));
                }
            }
        }
    }

    @Override
    public void cancel() {
        this.running = false;
    }
}
