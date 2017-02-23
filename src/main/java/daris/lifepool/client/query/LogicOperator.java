package daris.lifepool.client.query;

public enum LogicOperator {

    AND("&&"), OR("||");
    private String _symbol;

    LogicOperator(String symbol) {
        _symbol = symbol;
    }

    public String symbol() {
        return _symbol;
    }

    @Override
    public String toString() {
        return name().toLowerCase();
    }
}