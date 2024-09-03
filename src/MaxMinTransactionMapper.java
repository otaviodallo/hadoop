public class MaxMinTransactionMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    private Text countryYear = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] columns = line.split(";");
        
        if (!columns[0].equals("Country")) {
            String country = columns[0];
            String year = columns[1];
            if (country.equals("Brazil") && year.equals("2016")) {
                double price = Double.parseDouble(columns[5]);
                countryYear.set(country + "_" + year);
                context.write(countryYear, new DoubleWritable(price));
            }
        }
    }
}
