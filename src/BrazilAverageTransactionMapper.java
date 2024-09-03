public class BrazilAverageTransactionMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    private Text year = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] columns = line.split(";");
        
        if (!columns[0].equals("Country")) {
            String country = columns[0];
            if (country.equals("Brazil")) {
                String yearValue = columns[1];
                double price = Double.parseDouble(columns[5]);
                year.set(yearValue);
                context.write(year, new DoubleWritable(price));
            }
        }
    }
}
