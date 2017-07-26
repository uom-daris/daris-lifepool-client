package daris.lifepool.client.task;

import java.util.concurrent.Callable;
import java.util.logging.Level;
import java.util.logging.Logger;

import arc.mf.client.RemoteServer;
import arc.mf.client.ServerClient;
import daris.lifepool.client.connection.ConnectionSettings;

public abstract class Task<T> implements Callable<T> {

    private Logger _logger;

    protected ServerClient.Connection connect(ConnectionSettings settings) throws Throwable {
        RemoteServer server = new RemoteServer(settings.serverHost(), settings.serverPort(), settings.useHttp(),
                settings.encrypt());
        ServerClient.Connection cxn = server.open();
        if (settings.hasAuthenticationDetails()) {
            cxn.connect(settings.authenticationDetails());
        } else {
            cxn.reconnect(settings.sessionKey());
        }
        return cxn;
    }

    public void setLogger(Logger logger) {
        _logger = logger;
    }

    public Logger logger() {
        return _logger;
    }

    protected void log(Level level, String message, int indent, Throwable e) {
        StringBuilder sb = new StringBuilder();
        if (indent > 0) {
            for (int i = 0; i < indent; i++) {
                sb.append(' ');
            }
        }
        Logger logger = logger();
        if (logger != null) {
            sb.append(message);
            if (e != null) {
                logger.log(level, sb.toString(), e);
            } else {
                logger.log(level, sb.toString());
            }
        } else {
            if (Level.WARNING.equals(level) || Level.SEVERE.equals(level)) {
                System.out.println(sb.append(level.getName()).append(": ").append(message).toString());
                if (e != null) {
                    e.printStackTrace(System.err);
                }
            } else {
                System.out.println(sb.append(message).toString());
            }
        }
    }

    protected void logInfo(String message, int indent) {
        log(Level.INFO, message, indent, null);
    }

    protected void logInfo(String message) {
        log(Level.INFO, message, 0, null);
    }

    protected void logWarning(String message, int indent) {
        log(Level.WARNING, message, indent, null);
    }

    protected void logWarning(String message) {
        log(Level.WARNING, message, 0, null);
    }

    protected void logError(String message, int indent, Throwable e) {
        log(Level.SEVERE, message, indent, e);
    }

    protected void logError(String message, int indent) {
        log(Level.SEVERE, message, indent, null);
    }

    protected void logError(String message, Throwable e) {
        log(Level.SEVERE, message, 0, e);
    }

    protected void logError(String message) {
        log(Level.SEVERE, message, 0, null);
    }

}
