package daris.util;

public class CiteableIdUtils {

    public static String parent(String cid) {
        if (cid == null || cid.isEmpty()) {
            return null;
        }
        int idx = cid.lastIndexOf('.');
        if (idx == -1) {
            return null;
        }
        return cid.substring(0, idx);
    }

}
