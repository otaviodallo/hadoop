public class BrazilTransactionMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text country = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] columns = line.split(";");
        
        if (!columns[0].equals("Country")) { // Ignorar cabeçalho
            String countryName = columns[0];
            if (countryName.equals("Brazil")) {
                country.set(countryName);
                context.write(country, one);
            }
        }
    }
}
