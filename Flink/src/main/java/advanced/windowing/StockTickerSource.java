package advanced.windowing;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class StockTickerSource extends RichParallelSourceFunction {

    private boolean running = true;
    private static final Map<String, Double> stockTickersMap = new HashMap<>();

    static {
        stockTickersMap.put("MSFT", 200.0);
        stockTickersMap.put("AMZN", 3000.0);
        stockTickersMap.put("FB", 300.0);
        stockTickersMap.put("GOOG", 1500.0);
    }
    @Override
    public void run(SourceContext<StockPrices> sourceContext) throws Exception {
        Random rand = new Random();
        while(running) {
            long currTime = Instant.now().toEpochMilli();
            for(Map.Entry<String, Double> entry: stockTickersMap.entrySet()) {
                sourceContext.collect(new StockPrices(
                        entry.getKey(), entry.getValue() + (rand.nextGaussian() * 20), currTime
                ));
            }
            Thread.sleep(5000);
        }
    }

    @Override
    public void cancel() {
        this.running = false;
    }
}
