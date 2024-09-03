public class YearTransactionMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text year = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] columns = line.split(";");
        
        if (!columns[0].equals("Country")) {
            String yearValue = columns[1];  // Coluna de ano
            year.set(yearValue);
            context.write(year, one);
        }
    }
}
