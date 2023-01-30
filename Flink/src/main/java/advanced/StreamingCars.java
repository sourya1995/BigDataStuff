package advanced;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamingCars {
    public static class Car {
        public String brand;
        public String model;
        public String year;
        public Integer price;
        public Integer miles;

        public Car(){

        }

        public Car(String brand, String model, String year, Integer price, Integer miles) {
            this.brand = brand;
            this.model = model;
            this.year = year;
            this.price = price;
            this.miles = miles;
        }

        @Override
        public String toString() {
            return "Car{" +
                    "brand='" + brand + '\'' +
                    ", model='" + model + '\'' +
                    ", year='" + year + '\'' +
                    ", price='" + price + '\'' +
                    ", miles=" + miles +
                    '}';
        }
    }

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> carStream = env.readTextFile("file.txt");
        DataStream<String> filteredStream = carStream.filter((FilterFunction<String>) line -> !line.contains("brand, model, year, price, mileage") );
        DataStream<Car> carDetails = filteredStream.map(new MapFunction<String, Car>() {
            @Override
            public Car map(String row) throws Exception {
                String[] fields = row.split(",");
                return new Car(fields[0], fields[1], fields[2], Integer.parseInt(fields[3]), Integer.parseInt(fields[4]));
            }
        });

        DataStream<Car> filteredCarDetails = carDetails.filter(new RichFilterFunction<Car>() {

            private final IntCounter count = new IntCounter();
            @Override
            public boolean filter(Car car) throws Exception {
                if(car.brand.equals("bmw")){
                    count.add(1);

                    return true;
                }
                return false;
            }

            public void open(Configuration config){
                getRuntimeContext().addAccumulator("count", this.count);
            }
        });

        filteredCarDetails.print();
        JobExecutionResult result = filteredCarDetails.getExecutionEnvironment().execute();
        System.out.println("BMW Count: " + result.getAccumulatorResult("count"));
    }
}
