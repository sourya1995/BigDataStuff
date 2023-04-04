package CEP;

public class CloudPlatformStockRecord extends StockRecord{
    public CloudPlatformStockRecord(String ticker, String date, float closingPrice, String... tags) {
        super(ticker, date, closingPrice, tags);
    }
}
