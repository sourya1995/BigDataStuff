package CEP;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class StockRecord {
    private String ticker;
    private String date;
    private float closingPrice;
    private List<String> tags;

    public StockRecord(String ticker, String date, float closingPrice, String... tags) {
        this.ticker = ticker;
        this.date = date;
        this.closingPrice = closingPrice;
        this.tags = Arrays.asList(tags);
    }

    public String getTicker() {
        return ticker;
    }

    public void setTicker(String ticker) {
        this.ticker = ticker;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public float getClosingPrice() {
        return closingPrice;
    }

    public void setClosingPrice(float closingPrice) {
        this.closingPrice = closingPrice;
    }

    public List<String> getTags() {
        return tags;
    }

    public void setTags(List<String> tags) {
        this.tags = tags;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StockRecord that = (StockRecord) o;
        return Float.compare(that.closingPrice, closingPrice) == 0 && ticker.equals(that.ticker) && date.equals(that.date) && tags.equals(that.tags);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ticker, date, closingPrice, tags);
    }

    @Override
    public String toString() {
        return "StockRecord{" +
                "date='" + date + '\'' +
                ", closingPrice=" + closingPrice +
                '}';
    }
}
