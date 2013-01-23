package kafka.example;
/**
 * File: Producer.java
**/

import java.util.Properties;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

// import org.apache.logging.log4j.Logger;
import org.json.JSONObject;
import org.json.JSONException;
import kafka.javaapi.producer.ProducerData;
import kafka.producer.ProducerConfig;
import au.com.bytecode.opencsv.CSVReader;

/**
 * Kafka Producer for OAM. This producer processes a csv instances file and
 * publishes to Kafka.
 *
**/
public class Producer {
//    static Logger logger = LogManager.getLogger(Producer.class.getName());
    /**
     * Class constructor: Note this is a Utility class so it should not be
     * instantiated.
    **/
    protected Producer() {
        // prevents calls from subclass
        throw new UnsupportedOperationException();
    }

    /**
     * Constant: Kafka domain to use.
    **/
    private static final Integer NUMBER_OF_FIELDS = 2;
    /** Constant: The name. **/
    private static final int FIELD_NAME       = 0;
    /** Constant: The type. **/
    private static final int FIELD_TYPE         = 1;

    /**
     * Constructor for the OAMTopology.
     *
     * @param args only one arg. File path to instances.csv file.
    **/
    public static void main(final String[] args) {
        if (args.length != 1) {
            System.err.println("No file path was passed as an arg."
                + "\n(e.g. java -jar kafka_producer.jar /usr/.../file.csv).");
            System.exit(1);
        }


        try {
            Properties props = new Properties();
            props.put("zk.connect",
                getEnvVar("KAFKA_PRDCR_HOST")
                + ":" + getEnvVar("KAFKA_PRDCR_PORT"));
            props.put("serializer.class", "kafka.serializer.StringEncoder");
            props.put("producer.type", "async");
            props.put("batch.size", "500");
            props.put("compression.codec", "1");
            props.put("compression.topic", getEnvVar("KAFKA_TOPIC"));
            ProducerConfig config = new ProducerConfig(props);
            kafka.javaapi.producer.Producer<String, String> producer =

            new kafka.javaapi.producer.Producer<String, String>(config);

            CSVReader reader = new CSVReader(
                new FileReader(validateFilePath(args[0])));

            String [] nextLine;
            Integer count = 0;
            while ((nextLine = reader.readNext()) != null) {
                count++;
                ProducerData<String, String> data =
                    new ProducerData<String, String>(
                        getEnvVar("KAFKA_TOPIC"), convertRowToJSON(nextLine));

                producer.send(data);
            }

            producer.close();
            System.out.println("Sent " + count.toString()
                + " messages to Kafka successfully");
        } catch (IOException e) {
            System.err.println("Caught IOException: " + e.getMessage());
        }
    }

   /**
    * Takes an single instance row and converts to JSON format.
    *
    * @param row an single record in Array format.
    * @return JSON string
   **/
    public static String convertRowToJSON(final String [] row) {
        try {
            validateRowLength(row.length);
            JSONObject record = new JSONObject();

            // Populate data
            record.put("name", row[FIELD_NAME]);
            record.put("type",       Integer.parseInt(row[FIELD_TYPE]));

            return record.toString();

        } catch (JSONException e) {
            System.err.println("Caught JSONException: " + e.getMessage());
        } catch (IOException e) {
            System.err.println("Caught IOException: " + e.getMessage());
        }
        return null;
    }

   /**
    * Check to make sure the instance record has correct number of fields.
    *
    * @param rowLength length of array
    * @throws IOException if row length is incorrect
   **/
    private static void validateRowLength(final Integer rowLength)
        throws IOException {
        if (rowLength != NUMBER_OF_FIELDS) {
            throw new IOException("Instance record contains "
                + rowLength.toString() + " instead of "
                + NUMBER_OF_FIELDS + ".");
        }
    }

   /**
    * Check to make sure the environmental variable is set and return the
    * value.
    *
    * @param env variable to get the value of.
    * @return Environmental variable value.
    * @throws IOException if row length is incorrect
   **/
    private static String getEnvVar(final String env) throws IOException {
        String value = System.getenv(env);
        if (value == null) {
            throw new IOException(env + " environment variable is not set.");
        }

        return value;
    }

   /**
    * Check to make sure the file path of the instances.csv file is valid.
    *
    * @param filePath file path to validate.
    * @return the file path. It returns what was passed to it if valid.
    * @throws IOException file does not exist
   **/
    private static String validateFilePath(final String filePath)
        throws IOException {
        File file = new File(filePath);
        if (!file.exists()) {
            throw new IOException("'" + filePath
                + "' is not a vaild file path.");
        }

        return filePath;
    }
}
