package CEP;

import org.joda.time.Instant;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

public class StockRecord {
    private String ticker;
    private long timestamp;
    private float closingPrice;

    public StockRecord(String ticker, Instant instant, float closingPrice, List<String> tags) {
        this.ticker = ticker;
        this.timestamp = instant.getMillis();
        this.closingPrice = closingPrice;
        this.tags = tags;
    }

    private List<String> tags;

    @Override
    public String toString() {
        LocalDateTime eventDateTime = LocalDateTime.ofEpochSecond(this.timestamp/1000, 0, ZoneOffset.UTC);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd", Locale.ENGLISH);
        String formattedEventTime = eventDateTime.format(formatter);
        return "StockRecord{" +
                "ticker='" + ticker + '\'' +
                ", timestamp=" + timestamp +
                ", closingPrice=" + closingPrice +
                ", tags=" + tags +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StockRecord record = (StockRecord) o;
        return timestamp == record.timestamp && Float.compare(record.closingPrice, closingPrice) == 0 && Objects.equals(ticker, record.ticker) && Objects.equals(tags, record.tags);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ticker, timestamp, closingPrice, tags);
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getTimestamp() {
        return timestamp;
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

}
