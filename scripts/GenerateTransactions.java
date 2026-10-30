import java.io.BufferedWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

/**
 * Generates a customer-transaction CSV with a controlled share of bad rows and prints how many
 * of each kind it wrote, so an import can be checked against the expected outcome.
 *
 * <pre>
 * java scripts/GenerateTransactions.java [rows=1000000] [output=data/transactions.csv] [badRatio=0.01] [seed=42] [firstId=1]
 * </pre>
 */
public class GenerateTransactions {

    private static final String HEADER = "transaction_id,customer_id,amount,currency,type,timestamp,merchant,status";
    private static final String[] CURRENCIES = {"USD", "EUR", "GBP", "INR", "JPY", "AUD", "CAD", "SGD"};
    private static final String[] TYPES = {"DEBIT", "CREDIT"};
    private static final String[] STATUSES = {"PENDING", "COMPLETED", "FAILED", "REVERSED"};
    private static final String[] MERCHANTS = {"Amazon", "Walmart", "Starbucks", "Uber", "Netflix", "Shell",
            "\"Acme, Inc.\"", "Target", "Apple Store", "Local Grocer"};

    /** Each kind of bad row and the rejection reason the importer is expected to record for it. */
    enum Defect {
        MISSING_AMOUNT("VALIDATION_ERROR"),
        NON_NUMERIC_AMOUNT("VALIDATION_ERROR"),
        NEGATIVE_AMOUNT("VALIDATION_ERROR"),
        TOO_MANY_DECIMALS("VALIDATION_ERROR"),
        BAD_TIMESTAMP("VALIDATION_ERROR"),
        FUTURE_TIMESTAMP("VALIDATION_ERROR"),
        UNKNOWN_CURRENCY("VALIDATION_ERROR"),
        INVALID_TYPE("VALIDATION_ERROR"),
        INVALID_STATUS("VALIDATION_ERROR"),
        BAD_CUSTOMER_ID("VALIDATION_ERROR"),
        DEBIT_WITHOUT_MERCHANT("VALIDATION_ERROR"),
        TOO_FEW_COLUMNS("PARSE_ERROR"),
        TOO_MANY_COLUMNS("PARSE_ERROR"),
        BLANK_LINE("PARSE_ERROR"),
        COMMENT_LIKE_LINE("PARSE_ERROR"),
        DUPLICATE_ID("DUPLICATE");

        final String expectedReason;

        Defect(String expectedReason) {
            this.expectedReason = expectedReason;
        }
    }

    public static void main(String[] args) throws IOException {
        int rows = args.length > 0 ? Integer.parseInt(args[0]) : 1_000_000;
        Path output = Path.of(args.length > 1 ? args[1] : "data/transactions.csv");
        double badRatio = args.length > 2 ? Double.parseDouble(args[2]) : 0.01;
        long seed = args.length > 3 ? Long.parseLong(args[3]) : 42L;
        // Use a different firstId to generate a file whose transaction ids do not overlap another one.
        long firstId = args.length > 4 ? Long.parseLong(args[4]) : 1L;

        Random random = new Random(seed);
        OffsetDateTime base = OffsetDateTime.of(2025, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
        Defect[] defects = Defect.values();
        Map<Defect, Integer> counts = new TreeMap<>();
        List<String> recentIds = new ArrayList<>();
        int valid = 0;

        if (output.getParent() != null) {
            Files.createDirectories(output.getParent());
        }
        long started = System.nanoTime();
        try (BufferedWriter out = Files.newBufferedWriter(output, StandardCharsets.UTF_8)) {
            out.write(HEADER);
            out.newLine();
            for (int i = 1; i <= rows; i++) {
                String id = "TXN-%010d".formatted(firstId + i - 1);
                String customer = "CUST-%06d".formatted(1 + random.nextInt(200_000));
                String amount = BigDecimal.valueOf(1 + random.nextInt(500_000), 2).setScale(2, RoundingMode.UNNECESSARY).toPlainString();
                String currency = CURRENCIES[random.nextInt(CURRENCIES.length)];
                String type = TYPES[random.nextInt(TYPES.length)];
                String timestamp = base.plusSeconds(random.nextInt(60 * 60 * 24 * 600)).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
                String merchant = MERCHANTS[random.nextInt(MERCHANTS.length)];
                String status = STATUSES[random.nextInt(STATUSES.length)];

                String line;
                // Duplicates need an earlier id, so they cannot be the very first rows.
                if (random.nextDouble() < badRatio && !recentIds.isEmpty()) {
                    Defect defect = defects[random.nextInt(defects.length)];
                    counts.merge(defect, 1, Integer::sum);
                    line = switch (defect) {
                        case MISSING_AMOUNT -> row(id, customer, "", currency, type, timestamp, merchant, status);
                        case NON_NUMERIC_AMOUNT -> row(id, customer, "12.3O", currency, type, timestamp, merchant, status);
                        case NEGATIVE_AMOUNT -> row(id, customer, "-" + amount, currency, type, timestamp, merchant, status);
                        case TOO_MANY_DECIMALS -> row(id, customer, amount + "7", currency, type, timestamp, merchant, status);
                        case BAD_TIMESTAMP -> row(id, customer, amount, currency, type, "31/12/2025 10:00", merchant, status);
                        case FUTURE_TIMESTAMP -> row(id, customer, amount, currency, type, "2999-01-01T00:00:00Z", merchant, status);
                        case UNKNOWN_CURRENCY -> row(id, customer, amount, "XYZ", type, timestamp, merchant, status);
                        case INVALID_TYPE -> row(id, customer, amount, currency, "REFUND", timestamp, merchant, status);
                        case INVALID_STATUS -> row(id, customer, amount, currency, type, timestamp, merchant, "DONE");
                        case BAD_CUSTOMER_ID -> row(id, "C-" + random.nextInt(1000), amount, currency, type, timestamp, merchant, status);
                        case DEBIT_WITHOUT_MERCHANT -> row(id, customer, amount, currency, "DEBIT", timestamp, "", status);
                        case TOO_FEW_COLUMNS -> String.join(",", id, customer, amount, currency, type);
                        case TOO_MANY_COLUMNS -> row(id, customer, amount, currency, type, timestamp, merchant, status) + ",extra";
                        case BLANK_LINE -> "";
                        case COMMENT_LIKE_LINE -> "# exported by legacy system " + i;
                        case DUPLICATE_ID -> row(recentIds.get(random.nextInt(recentIds.size())), customer, amount,
                                currency, type, timestamp, merchant, status);
                    };
                } else {
                    line = row(id, customer, amount, currency, type, timestamp, merchant, status);
                    valid++;
                    if (recentIds.size() < 10_000) {
                        recentIds.add(id);
                    } else {
                        recentIds.set(random.nextInt(recentIds.size()), id);
                    }
                }
                out.write(line);
                out.newLine();
            }
        }

        Map<String, Integer> expectedByReason = new TreeMap<>();
        counts.forEach((defect, n) -> expectedByReason.merge(defect.expectedReason, n, Integer::sum));
        int bad = counts.values().stream().mapToInt(Integer::intValue).sum();
        System.out.printf("Wrote %s: %,d data lines (+ header) in %d ms%n", output, rows,
                (System.nanoTime() - started) / 1_000_000);
        System.out.printf("  valid rows (expected inserted): %,d%n", valid);
        System.out.printf("  bad rows:                       %,d%n", bad);
        counts.forEach((defect, n) -> System.out.printf("    %-24s %,7d -> %s%n", defect, n, defect.expectedReason));
        System.out.println("  expected rejections by reason: " + expectedByReason);
    }

    private static String row(String... fields) {
        return String.join(",", fields);
    }
}
