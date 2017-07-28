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

    public static String parent(String cid, int depth) {
        String pid = cid;
        for (int i = 0; i < depth; i++) {
            pid = parent(pid);
            if (pid == null) {
                break;
            }
        }
        return pid;
    }

    public static int compare(String id1, String id2) {
        if (id1 == null && id2 == null) {
            return 0;
        }
        if (id1 != null && id2 == null) {
            return 1;
        }
        if (id1 == null && id2 != null) {
            return -1;
        }
        if (id1.equals(id2)) {
            return 0;
        }
        String[] parts1 = id1.split("\\.");
        String[] parts2 = id2.split("\\.");
        if (parts1.length < parts2.length) {
            return -1;
        }
        if (parts1.length > parts2.length) {
            return 1;
        }
        for (int i = 0; i < parts1.length; i++) {
            if (!parts1[i].equals(parts2[i])) {
                long n1 = Long.parseLong(parts1[i]);
                long n2 = Long.parseLong(parts2[i]);
                if (n1 < n2) {
                    return -1;
                }
                if (n1 > n2) {
                    return 1;
                }
            }
        }
        return 0;
    }
}
