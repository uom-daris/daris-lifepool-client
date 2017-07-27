package daris.lifepool.client;

public class Applications {

    public static final String DICOM_AE_TITLE = "DARIS_LIFEPOOL_CLIENT";
    
    public static final String PROPERTIES_FILE = new StringBuilder()
            .append(System.getProperty("user.home").replace('\\', '/')).append("/.daris/daris-lifepool-client.properties").toString();

}
