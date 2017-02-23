package daris.lifepool.client.query;

public enum Operator {

    EQUALS("="), NOT_EQUALS("!=");

    private String _symbol;

    Operator(String symbol) {
        _symbol = symbol;
    }

    public String symbol() {
        return _symbol;
    }

    @Override
    public String toString() {
        return symbol();
    }
}