package daris.util;

public class ByteUtils {

    public static String getHumanReadableSize(long size, boolean i, int fraction) {
        long unit = i ? 1024 : 1000;
        if (size < unit) {
            return String.valueOf(size) + (size > 1 ? " Bytes" : " Byte");
        }
        long exp = (long) (Math.log(size) / Math.log(unit));
        double value = size / Math.pow(unit, exp);
        return String.format("%." + fraction + "f %s%s", value, "KMGTPEZY".charAt((int) exp - 1), i ? "iB" : "B");
    }

    public static String getHumanReadableSize(long size, boolean i) {
        return getHumanReadableSize(size, i, 3);
    }

    public static String getHumanReadableSize(long size) {
        return getHumanReadableSize(size, false, 3);
    }

}
