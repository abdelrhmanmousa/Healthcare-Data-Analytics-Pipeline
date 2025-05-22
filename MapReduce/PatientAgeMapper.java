import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class PatientAgeMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {

    private final static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final Date referenceDate;

    static {
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        try {
            referenceDate = sdf.parse("2008-01-01 00:00:00");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        // Skip header
        if (line.startsWith("ROW_ID")) return;

        String[] parts = line.split(",", -1);
        if (parts.length < 5) return;

        try {
            Date dob = sdf.parse(parts[3]);
            Date dod = parts[4].isEmpty() ? referenceDate : sdf.parse(parts[4]);

            float age = (float) ((dod.getTime() - dob.getTime()) / (1000.0 * 60 * 60 * 24 * 365.25));

            if (age > 0 && age < 120) {
                context.write(new Text("age"), new FloatWritable(age));
            }

        } catch (Exception e) {
            // skip malformed rows
        }
    }
}
