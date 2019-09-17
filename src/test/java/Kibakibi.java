import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class Kibakibi {

    public static void main(String[] args) {

        try {
//            String string = new String(
//                    Files.readAllBytes(
//                            Paths.get("/home/samim/Downloads/Module-3 Assignment Dataset - Find the Hot and Cold Days/WeatherData.txt")
//                    )
//            );
//
//            StringTokenizer tokenizer = new StringTokenizer(string, "\n");
//
//            String[] split = tokenizer.nextToken().split("\\s+");
//
//            float max = Float.parseFloat(split[5]);
//
//            if (max > 10) {
//                System.out.println("Yo  yo " + max);
//            } else {
//                System.out.println("No " + max);
//            }

            float fff = (float) -9999.9;
            System.out.println(fff);


            DateTimeFormatter inputFormat = DateTimeFormatter.ofPattern("yyyyMMdd");
            DateTimeFormatter outputFormat = DateTimeFormatter.ofPattern("dd-MM-yyyy");

            String str = LocalDate.parse("20130631", inputFormat).format(outputFormat);

            System.out.println(str);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
